// ===================================================================
// UNLEASHED API INTEGRATION SCRIPT - GOOGLE APPS SCRIPT COMPATIBLE
// ===================================================================

// === CONFIGURATION ===
function getConfig() {
  var props = PropertiesService.getScriptProperties();
  return {
    API_ID: props.getProperty('UNLEASHED_API_ID') || '017f4e23-867e-4d9a-8d7b-3fd4e4ac8abe',
    API_KEY: props.getProperty('UNLEASHED_API_KEY') || 'jgrsef5CsuP67zVBzEnZy0s4R7knG9rWTp3kbzCFQIYHKipxNhGH4UQkmkA0LcRMvGAg0FggyarV5gdbcXBOQ==',
    CLIENT_TYPE: 'GoogleAppsScript',
    BASE_URL: 'https://api.unleashedsoftware.com',
    WAREHOUSES: ["Mont", "EntEff", "CannWare", "BHC", "Aeris", "BLS"], // UPDATED: Corrected warehouse code
    MAX_RETRIES: 3,
    RATE_LIMIT_MS: 125 // 8 requests per second
  };
}

// === GLOBAL CACHE ===
var productPriceCache = {};
var lastRequestTime = 0;

// === UTILITY FUNCTIONS ===
function roundToTwoDecimals(num) {
  return Math.round(num * 100) / 100;
}

function formatDuration(milliseconds) {
  var seconds = Math.floor(milliseconds / 1000);
  var minutes = Math.floor(seconds / 60);
  var hours = Math.floor(minutes / 60);
  
  if (hours > 0) {
    return hours + 'h ' + (minutes % 60) + 'm ' + (seconds % 60) + 's';
  } else if (minutes > 0) {
    return minutes + 'm ' + (seconds % 60) + 's';
  } else {
    return seconds + 's';
  }
}

function respectRateLimit() {
  var config = getConfig();
  var now = Date.now();
  var timeSinceLastRequest = now - lastRequestTime;
  
  if (timeSinceLastRequest < config.RATE_LIMIT_MS) {
    var sleepTime = config.RATE_LIMIT_MS - timeSinceLastRequest;
    Logger.log('⏱️ Rate limiting: sleeping for ' + sleepTime + 'ms');
    Utilities.sleep(sleepTime);
  }
  lastRequestTime = Date.now();
}

/**
 * Parses an Unleashed legacy date string and formats it as YYYY-MM-DD.
 * e.g., "/Date(1756598400000)/" -> "2025-08-26"
 */
function parseAndFormatUnleashedDate(dateString) {
  if (!dateString || typeof dateString !== 'string') {
    return null;
  }
  
  var match = dateString.match(/\/Date\((\d+)\)\//);
  if (match && match[1]) {
    var timestamp = parseInt(match[1], 10);
    var date = new Date(timestamp);
    return Utilities.formatDate(date, Session.getScriptTimeZone(), 'yyyy-MM-dd');
  }
  
  // Return the original string if it's already in a valid format or not the expected one
  return dateString; 
}

// --- NEW HELPERS (for picking current-month order) ---
function parseUnleashedDateToJs(dateValue) {
  if (!dateValue) return null;
  if (typeof dateValue === 'string') {
    var m = dateValue.match(/\/Date\((\d+)\)\//);
    if (m) {
      var d = new Date(parseInt(m[1], 10));
      return isNaN(d) ? null : d;
    }
    var d2 = new Date(dateValue);
    return isNaN(d2) ? null : d2;
  }
  if (dateValue instanceof Date) return dateValue;
  return null;
}

function getOrderDate(order) {
  return (
    parseUnleashedDateToJs(order.OrderDate) ||
    parseUnleashedDateToJs(order.SalesOrderDate) ||
    parseUnleashedDateToJs(order.DeliveryDate)
  );
}

function pickMonthlyOrder(orders) {
  if (!orders || orders.length === 0) return null;

  var now = new Date();
  var y = now.getFullYear();
  var m = now.getMonth(); // 0-based

  // Prefer orders whose date falls in the current month
  var inCurrentMonth = orders.filter(function(o) {
    var d = getOrderDate(o);
    return d && d.getFullYear() === y && d.getMonth() === m;
  });

  var pool = inCurrentMonth.length ? inCurrentMonth : orders.slice();

  // From the chosen pool, take the most recent by date
  pool.sort(function(a, b) {
    var ad = getOrderDate(a) || new Date(0);
    var bd = getOrderDate(b) || new Date(0);
    return bd - ad;
  });

  return pool[0] || null;
}

// === SIGNATURE GENERATOR ===
function getUnleashedSignature(paramsArray, apiKey) {
  if (!Array.isArray(paramsArray)) {
    Logger.log("⚠️ getUnleashedSignature received invalid input:", paramsArray);
    throw new Error("Invalid signature input");
  }
  var sortedQuery = paramsArray.slice().sort().join('&');
  var raw = Utilities.computeHmacSha256Signature(sortedQuery, apiKey);
  return Utilities.base64Encode(raw);
}

// === API REQUEST WITH RETRY ===
function makeApiRequest(endpoint, params, method, payload) {
  params = params || [];
  method = method || 'GET';
  
  var config = getConfig();
  var maxRetries = config.MAX_RETRIES;
  var lastError;

  for (var attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      respectRateLimit();
      
      var signature = getUnleashedSignature(params, config.API_KEY);
      var url = params.length > 0 
        ? config.BASE_URL + endpoint + '?' + params.join('&')
        : config.BASE_URL + endpoint;

      var headers = {
        'api-auth-id': config.API_ID,
        'api-auth-signature': signature,
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'client-type': config.CLIENT_TYPE
      };

      var options = {
        method: method.toLowerCase(),
        headers: headers,
        muteHttpExceptions: true
      };

      if (payload && (method === 'POST' || method === 'PUT')) {
        options.payload = JSON.stringify(payload);
      }

      Logger.log('📡 API Request attempt ' + attempt + '/' + maxRetries + ': ' + method + ' ' + url);
      var response = UrlFetchApp.fetch(url, options);
      var statusCode = response.getResponseCode();
      
      if (statusCode >= 200 && statusCode < 300) {
        return response;
      } else if (statusCode >= 500 || statusCode === 429) {
        throw new Error('HTTP ' + statusCode + ': ' + response.getContentText());
      } else {
        return response; // Don't retry on client errors
      }
    } catch (error) {
      lastError = error;
      if (attempt < maxRetries) {
        var backoffTime = 1000 * Math.pow(2, attempt - 1);
        Logger.log('⚠️ Request failed, retrying in ' + backoffTime + 'ms: ' + error.message);
        Utilities.sleep(backoffTime);
      }
    }
  }
  
  throw new Error('Request failed after ' + maxRetries + ' attempts: ' + lastError.message);
}

// === PRODUCT PRICE FETCHER ===
function fetchProductDetails(productCode) {
  var cacheKey = 'product_' + productCode;
  
  if (productPriceCache[cacheKey] !== undefined) {
    Logger.log('💾 Cache hit for product: ' + productCode);
    return productPriceCache[cacheKey];
  }

  Logger.log('🔎 Fetching product details for: ' + productCode);
  try {
    var params = ['productCode=' + encodeURIComponent(productCode)];
    var response = makeApiRequest('/Products', params);
    
    if (response.getResponseCode() !== 200) {
      Logger.log('❌ Failed to fetch product ' + productCode + ': ' + response.getResponseCode());
      productPriceCache[cacheKey] = null;
      return null;
    }

    var data = JSON.parse(response.getContentText());
    var product = data && data.Items && data.Items[0];
    
    if (product && product.DefaultSellPrice) {
      productPriceCache[cacheKey] = product.DefaultSellPrice;
      Logger.log('✅ Cached price for ' + productCode + ': ' + product.DefaultSellPrice);
      return product.DefaultSellPrice;
    }
    
    Logger.log('⚠️ No default sell price found for ' + productCode);
    productPriceCache[cacheKey] = null;
    return null;
  } catch (error) {
    Logger.log('❌ Error fetching product ' + productCode + ': ' + error.message);
    productPriceCache[cacheKey] = null;
    return null;
  }
}

// === VALIDATE FEED ROW ===
function validateFeedRow(row, headers) {
  var required = ['ProductCode', 'WarehouseCode', 'Quantity'];
  
  for (var i = 0; i < required.length; i++) {
    var field = required[i];
    var index = headers.indexOf(field);
    var value = row[index];
    if (!value || value === '') {
      Logger.log('⚠️ Missing required field: ' + field);
      return false;
    }
  }
  
  var qtyIndex = headers.indexOf('Quantity');
  var quantity = Number(row[qtyIndex]);
  if (isNaN(quantity) || quantity <= 0) {
    Logger.log('⚠️ Invalid quantity: ' + row[qtyIndex]);
    return false;
  }
  
  return true;
}

// === STOCK ON HAND UPDATE ===
function updateStockOnHandByWarehouse() {
  var startTime = Date.now();
  Logger.log('🚀 Starting stock on hand update...');
  
  try {
    var config = getConfig();
    
    // Setup spreadsheet
    var SHEET_NAME = "SOH";
    var ss = SpreadsheetApp.getActiveSpreadsheet();
    var sheet = ss.getSheetByName(SHEET_NAME);
    if (!sheet) sheet = ss.insertSheet(SHEET_NAME);
    else sheet.clearContents();

    var headers = ["Warehouse", "Product Code", "Description", "Qty On Hand", "Qty Available", "Qty On Purchase"];
    sheet.getRange(1, 1, 1, headers.length).setValues([headers]);

    var allRows = [];
    var totalRowsFetched = 0;

    // Process each warehouse
    for (var w = 0; w < config.WAREHOUSES.length; w++) {
      var warehouseCode = config.WAREHOUSES[w];
      Logger.log('📦 Processing warehouse: ' + warehouseCode + ' (' + (w + 1) + '/' + config.WAREHOUSES.length + ')');
      
      var page = 1;
      var totalPages = 1;
      var warehouseRowsFetched = 0;

      try {
        do {
          var params = ['page=' + page, 'warehouseCode=' + warehouseCode];
          var response = makeApiRequest('/StockOnHand', params);
          
          if (response.getResponseCode() !== 200) {
            throw new Error('API error: ' + response.getResponseCode() + ' - ' + response.getContentText());
          }
          
          var data = JSON.parse(response.getContentText());
          var items = data && data.Items ? data.Items : [];
          totalPages = data && data.Pagination && data.Pagination.NumberOfPages ? data.Pagination.NumberOfPages : 1;

          Logger.log('-> Page ' + page + '/' + totalPages + ' for ' + warehouseCode + ': ' + items.length + ' items');

          for (var i = 0; i < items.length; i++) {
            var item = items[i];
            var qoh = item.QuantityOnHand || item.QtyOnHand || 0;
            var avail = item.QuantityAvailable || item.AvailableQty || 0;
            var onPurchase = item.QuantityOnPurchase || item.OnPurchase || 0;

            if (qoh > 0 || avail > 0 || onPurchase > 0) {
              allRows.push([
                warehouseCode,
                item.ProductCode || '',
                item.ProductDescription || '',
                qoh,
                avail,
                onPurchase
              ]);
              totalRowsFetched++;
              warehouseRowsFetched++;
            }
          }

          page++;
        } while (page <= totalPages);
        
        Logger.log('✅ Completed warehouse ' + warehouseCode + ': ' + warehouseRowsFetched + ' products found');
      } catch (error) {
        Logger.log('🚨 Failed to process warehouse ' + warehouseCode + ': ' + error.message);
        continue;
      }
    }

    // Write results to sheet
    if (allRows.length > 0) {
      sheet.getRange(2, 1, allRows.length, headers.length).setValues(allRows);
      var duration = formatDuration(Date.now() - startTime);
      var message = '✅ Imported ' + totalRowsFetched + ' stock rows across ' + config.WAREHOUSES.length + ' warehouses in ' + duration;
      Logger.log(message);
      SpreadsheetApp.getUi().alert(message);
    } else {
      var message = "⚠️ No stock data returned from any warehouse";
      Logger.log(message);
      SpreadsheetApp.getUi().alert(message);
    }
    
  } catch (error) {
    Logger.log('❌ Critical error in updateStockOnHandByWarehouse: ' + error.message);
    SpreadsheetApp.getUi().alert('❌ Error: ' + error.message);
  }
}

// === SALES ORDER SYNC ===
function updateSalesOrdersFromFeed() {
  var startTime = Date.now();
  Logger.log('🚀 Starting sales order sync from feed...');
  
  try {
    // Read feed sheet
    var FEED_SHEET = 'feed';
    var sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(FEED_SHEET);
    if (!sheet) {
      throw new Error("Sheet 'feed' not found");
    }

    var values = sheet.getDataRange().getValues();
    if (values.length <= 1) {
      throw new Error("Feed sheet is empty or contains only headers");
    }

    var headers = values.shift();
    var codeIdx = headers.indexOf("ProductCode");
    var warehouseIdx = headers.indexOf("WarehouseCode");
    var qtyIdx = headers.indexOf("Quantity");

    if (codeIdx === -1 || warehouseIdx === -1 || qtyIdx === -1) {
      throw new Error("Required columns not found: ProductCode, WarehouseCode, Quantity");
    }

    // Configuration mappings
    var customerMap = {
      "entourage effect": "The Entourage Effect",
      "montu": "Montu Group Pty Ltd",
      "cannabis warehouse": "Cannabis Warehouse Australia Pty Ltd",
      "burleigh heads": "Burleigh Heads Cannabis",
      "aeris health": "Aeris Health",
      "bls": "Breathe Life Sciences"
    };
    
    // Filter valid rows and get unique products
    var validRows = [];
    var uniqueProductCodes = [];
    
    for (var i = 0; i < values.length; i++) {
      if (validateFeedRow(values[i], headers)) {
        validRows.push(values[i]);
        var productCode = values[i][codeIdx];
        if (uniqueProductCodes.indexOf(productCode) === -1) {
          uniqueProductCodes.push(productCode);
        }
      }
    }
    
    Logger.log('📋 Found ' + validRows.length + ' valid rows with ' + uniqueProductCodes.length + ' unique products');
    
    // Pre-fetch product prices
    Logger.log('💰 Pre-fetching product prices...');
    for (var p = 0; p < uniqueProductCodes.length; p++) {
      var productCode = uniqueProductCodes[p];
      fetchProductDetails(productCode);
      Logger.log('-> Progress: ' + (p + 1) + '/' + uniqueProductCodes.length + ' (' + Math.round(((p + 1) / uniqueProductCodes.length) * 100) + '%)');
    }

    // Build order map
    var orderMap = {};
    for (var v = 0; v < validRows.length; v++) {
      var row = validRows[v];
      var product = row[codeIdx];
      var warehouse = row[warehouseIdx];
      var qty = Number(row[qtyIdx]);
      
      var normalizedWarehouseName = String(warehouse).trim().toLowerCase();
      var custCode = customerMap[normalizedWarehouseName];

      if (!custCode) {
        Logger.log('⚠️ Unknown warehouse: ' + warehouse);
        continue;
      }

      var unitPrice = productPriceCache['product_' + product];
      if (unitPrice === undefined || unitPrice === null) {
        Logger.log('⚠️ No price found for product: ' + product);
        continue;
      }
      
      if (!orderMap[custCode]) {
        orderMap[custCode] = [];
      }

      var lineTotal = roundToTwoDecimals(qty * unitPrice);
      var lineTax = roundToTwoDecimals(lineTotal * 0.1);

      orderMap[custCode].push({
        Product: { ProductCode: product },
        OrderQuantity: qty,
        UnitPrice: unitPrice,
        LineTotal: lineTotal,
        LineTax: lineTax,
        TaxRate: 0.1,
      });
    }

    var orderKeys = Object.keys(orderMap);
    Logger.log('📦 Created ' + orderKeys.length + ' unique orders from feed');

    // Fetch existing parked orders
    Logger.log('🔍 Fetching existing parked orders...9');
    var parkedByCustomer = {}; // changed: keep ALL parked orders per customer
    var page = 1;
    var totalPages = 1;

    do {
      var params = ['orderStatus=Parked', 'page=' + page];
      var response = makeApiRequest('/SalesOrders', params);
      
      if (response.getResponseCode() !== 200) {
        throw new Error('Failed to fetch parked orders: ' + response.getResponseCode());
      }

      var data = JSON.parse(response.getContentText());
      var items = data && data.Items ? data.Items : [];
      totalPages = data && data.Pagination && data.Pagination.NumberOfPages ? data.Pagination.NumberOfPages : 1;
      
      items.forEach(function(order) {
        if (order.Customer && order.Customer.CustomerCode) {
          var code = order.Customer.CustomerCode;
          if (!parkedByCustomer[code]) parkedByCustomer[code] = [];
          parkedByCustomer[code].push(order);
        }
      });
      page++;
    } while (page <= totalPages);

    Logger.log('📋 Found parked orders for ' + Object.keys(parkedByCustomer).length + ' customers');
    
    // Process each order
    for (var o = 0; o < orderKeys.length; o++) {
      var custCode = orderKeys[o];
      var feedLines = orderMap[custCode];
      var candidates = parkedByCustomer[custCode] || [];
      var existingOrder = pickMonthlyOrder(candidates); // changed: choose current-month order (fallback: most recent)
      
      Logger.log('🔄 Processing order ' + (o + 1) + '/' + orderKeys.length + ' for customer: ' + custCode);
      
      if (!existingOrder) {
          Logger.log('❌ No existing parked order found for ' + custCode + '. Skipping.');
          continue;
      }
      
      // Update order header notes and totals
      var now = new Date();
      var syncNote = 'Last synced from Google Sheet on ' + now.toLocaleDateString() + ' at ' + now.toLocaleTimeString() + '.';
      
      // FIX: Clone the existing order and modify it. This is necessary because PUT requires a full object.
      var orderHeaderPayload = JSON.parse(JSON.stringify(existingOrder));
      orderHeaderPayload.SalesOrderNotes = existingOrder.SalesOrderNotes ? existingOrder.SalesOrderNotes + '\n' + syncNote : syncNote;
      
      // FIX: Convert legacy date fields to a valid format
      orderHeaderPayload.SalesOrderDate = parseAndFormatUnleashedDate(orderHeaderPayload.SalesOrderDate);
      orderHeaderPayload.DeliveryDate = parseAndFormatUnleashedDate(orderHeaderPayload.DeliveryDate);
      orderHeaderPayload.OrderDate = parseAndFormatUnleashedDate(orderHeaderPayload.OrderDate);
      
      // Calculate new totals for header update
      var subTotal = feedLines.reduce(function(sum, line) { return sum + line.LineTotal; }, 0);
      var taxTotal = feedLines.reduce(function(sum, line) { return sum + line.LineTax; }, 0);
      orderHeaderPayload.SubTotal = roundToTwoDecimals(subTotal);
      orderHeaderPayload.TaxTotal = roundToTwoDecimals(taxTotal);
      orderHeaderPayload.Total = roundToTwoDecimals(subTotal + taxTotal);
      
      Logger.log('📤 Sending PUT payload to update order header for ' + custCode + '. New SubTotal: ' + orderHeaderPayload.SubTotal);
      var headerResponse = makeApiRequest('/SalesOrders/' + existingOrder.Guid, [], 'PUT', orderHeaderPayload);
      
      if (headerResponse.getResponseCode() >= 200 && headerResponse.getResponseCode() < 300) {
          Logger.log('✅ Updated order header for ' + custCode);
      } else {
          Logger.log('❌ Failed to update order header for ' + custCode + ': ' + headerResponse.getResponseCode() + ' - ' + headerResponse.getContentText());
      }
      
      // Update line items individually
      var existingLines = existingOrder.SalesOrderLines || [];
      
      for (var f = 0; f < feedLines.length; f++) {
        var feedLine = feedLines[f];
        var existingLine = existingLines.find(function(line) {
          return line.Product && line.Product.ProductCode === feedLine.Product.ProductCode;
        });

        if (existingLine) {
            // FIX: Add the line item Guid to the payload to prevent mismatch error
            var linePayload = {
                Guid: existingLine.Guid,
                OrderQuantity: feedLine.OrderQuantity,
                UnitPrice: feedLine.UnitPrice
            };
            
            var lineEndpoint = '/SalesOrders/' + existingOrder.Guid + '/Lines/' + existingLine.Guid;
            
            Logger.log('  -> Updating line item for ' + feedLine.Product.ProductCode + ' with new quantity ' + feedLine.OrderQuantity);
            var lineResponse = makeApiRequest(lineEndpoint, [], 'PUT', linePayload);
            
            if (lineResponse.getResponseCode() >= 200 && lineResponse.getResponseCode() < 300) {
                Logger.log('  ✅ Line updated successfully.');
            } else {
                Logger.log('  ❌ Failed to update line: ' + lineResponse.getResponseCode() + ' - ' + lineResponse.getContentText());
            }
        } else {
            Logger.log('  ⚠️ Warning: No existing line found for ' + feedLine.Product.ProductCode + '. Cannot update.');
        }
      }
    }

    var duration = formatDuration(Date.now() - startTime);
    var message = '✅ Sales order sync completed in ' + duration + '. Check logs for details.';
    Logger.log(message);
    SpreadsheetApp.getUi().alert(message);
    
  } catch (error) {
    Logger.log('❌ Critical error in updateSalesOrdersFromFeed: ' + error.message);
    SpreadsheetApp.getUi().alert('❌ Error: ' + error.message);
  }
}

// === MENU SYSTEM ===
function onOpen() {
  var ui = SpreadsheetApp.getUi();
  ui.createMenu('🌿 Orders')
    .addItem('📦 Update Stock On Hand (All Warehouses)', 'updateStockOnHandByWarehouse')
    .addItem('📤 Sync External Feed to Sales Orders', 'updateSalesOrdersFromFeed')
    .addSeparator()
    .addItem('⚙️ Setup API Credentials', 'setupCredentials')
    .addItem('🔧 Clear Cache', 'clearCache')
    .addToUi();
}

// === UTILITY MENU FUNCTIONS ===
function setupCredentials() {
  var ui = SpreadsheetApp.getUi();
  var result = ui.prompt('Setup API Credentials', 'Enter your Unleashed API ID:', ui.ButtonSet.OK_CANCEL);
  
  if (result.getSelectedButton() === ui.Button.OK) {
    var apiId = result.getResponseText();
    var apiKeyResult = ui.prompt('Setup API Credentials', 'Enter your Unleashed API Key:', ui.ButtonSet.OK_CANCEL);
    
    if (apiKeyResult.getSelectedButton() === ui.Button.OK) {
      PropertiesService.getScriptProperties().setProperties({
        'UNLEASHED_API_ID': apiId,
        'UNLEASHED_API_KEY': apiKeyResult.getResponseText()
      });
      ui.alert('✅ API credentials saved securely!');
    }
  }
}

function clearCache() {
  productPriceCache = {};
  SpreadsheetApp.getUi().alert('✅ Cache cleared successfully!');
}
