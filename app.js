const express = require('express');
const axios = require('axios');
const cron = require('node-cron');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 8080;

// Middleware
app.use(express.json());

// Service URLs
const SCRAPER_URL = process.env.SCRAPER_URL || 'https://cloud-scraper-smg-production.up.railway.app';
const PIPELINE_URL = process.env.PIPELINE_URL || 'https://smg-pipeline-phase2-production.up.railway.app';

// TEST PAGE ENDPOINT
app.get('/test', (req, res) => {
  res.send(`
<!DOCTYPE html>
<html>
<head>
    <title>SMG Orchestrator Tester</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background: #f9f9f9; }
        .module { border: 1px solid #ccc; padding: 20px; margin: 10px 0; background: white; border-radius: 5px; }
        button { background: #007cba; color: white; padding: 10px 20px; border: none; cursor: pointer; border-radius: 3px; margin: 5px; }
        button:hover { background: #005a87; }
        .result { background: #f5f5f5; padding: 10px; margin: 10px 0; white-space: pre-wrap; font-family: monospace; border: 1px solid #ddd; max-height: 400px; overflow-y: auto; }
        textarea { width: 100%; height: 60px; font-family: monospace; }
        .status { padding: 10px; margin: 10px 0; border-radius: 3px; }
        .success { background: #d4edda; color: #155724; border: 1px solid #c3e6cb; }
        .error { background: #f8d7da; color: #721c24; border: 1px solid #f5c6cb; }
        .warning { background: #fff3cd; color: #856404; border: 1px solid #ffeaa7; }
    </style>
</head>
<body>
    <h1>🚀 SMG Orchestrator Test Center</h1>
    <div class="status success">✅ Orchestrator running! Test the complete SMG automation below:</div>
    <div class="status warning">🔍 FIELD NAME FIX: Now checks for csvContent field from scraper!</div>
    
    <div class="module">
        <h3>📊 System Status Check</h3>
        <p>Test connectivity to all services (Scraper + Pipeline + Orchestrator):</p>
        <button onclick="testStatus()">🔍 Check System Status</button>
        <div id="statusResult" class="result">Click button to test...</div>
    </div>
    
    <div class="module">
        <h3>📅 Daily Orchestration Test</h3>
        <p>Test complete daily flow (yesterday's data scraping + pipeline processing):</p>
        <div class="warning">⚠️ This will call the real scraper and pipeline - use carefully!</div>
        <button onclick="testDaily()">🔄 Test Daily Flow</button>
        <div id="dailyResult" class="result">Click button to test...</div>
    </div>
    
    <div class="module">
        <h3>📋 Backfill Test</h3>
        <p>Test backfill orchestration for specific dates:</p>
        <label>Start Date:</label> <input type="date" id="startDate" value="2025-06-17">
        <label>End Date:</label> <input type="date" id="endDate" value="2025-06-18">
        <br><br>
        <div class="warning">⚠️ This will process real data - start with small date ranges!</div>
        <button onclick="testBackfill()">📊 Test Backfill</button>
        <div id="backfillResult" class="result">Click button to test...</div>
    </div>

    <div class="module">
        <h3>🔗 Service Links</h3>
        <p>
            <a href="/status" target="_blank">Orchestrator Status</a> | 
            <a href="${SCRAPER_URL}" target="_blank">Phase 1: Scraper</a> | 
            <a href="${PIPELINE_URL}" target="_blank">Phase 2: Pipeline</a>
        </p>
        <p><strong>Architecture:</strong> Phase 1 (Scraper) → Phase 3 (Orchestrator) → Phase 2 (Pipeline)</p>
    </div>

    <script>
        const baseUrl = window.location.origin;
        
        async function testStatus() {
            const resultDiv = document.getElementById('statusResult');
            resultDiv.textContent = '🔍 Checking system status...';
            resultDiv.className = 'result';
            
            try {
                const response = await fetch(baseUrl + '/status');
                const result = await response.json();
                
                if (response.ok) {
                    resultDiv.textContent = '✅ Status Check Success:\\n\\n' + JSON.stringify(result, null, 2);
                    resultDiv.style.background = '#d4edda';
                } else {
                    resultDiv.textContent = '❌ Status Check Error:\\n\\n' + JSON.stringify(result, null, 2);
                    resultDiv.style.background = '#f8d7da';
                }
            } catch (error) {
                resultDiv.textContent = '❌ Network Error: ' + error.message;
                resultDiv.style.background = '#f8d7da';
            }
        }
        
        async function testDaily() {
            const resultDiv = document.getElementById('dailyResult');
            resultDiv.textContent = '🔄 Running daily orchestration...\\nThis may take several minutes...';
            resultDiv.className = 'result';
            
            try {
                const response = await fetch(baseUrl + '/orchestrate/daily', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' }
                });
                
                const result = await response.json();
                
                if (response.ok) {
                    resultDiv.textContent = '✅ Daily Orchestration Success:\\n\\n' + JSON.stringify(result, null, 2);
                    resultDiv.style.background = '#d4edda';
                } else {
                    resultDiv.textContent = '❌ Daily Orchestration Error:\\n\\n' + JSON.stringify(result, null, 2);
                    resultDiv.style.background = '#f8d7da';
                }
            } catch (error) {
                resultDiv.textContent = '❌ Network Error: ' + error.message;
                resultDiv.style.background = '#f8d7da';
            }
        }
        
        async function testBackfill() {
            const startDate = document.getElementById('startDate').value;
            const endDate = document.getElementById('endDate').value;
            const resultDiv = document.getElementById('backfillResult');
            
            if (!startDate || !endDate) {
                resultDiv.textContent = '❌ Please select both start and end dates';
                resultDiv.style.background = '#f8d7da';
                return;
            }
            
            resultDiv.textContent = `📊 Running backfill orchestration for ${startDate} to ${endDate}...\\nThis may take several minutes...`;
            resultDiv.className = 'result';
            
            try {
                const response = await fetch(baseUrl + '/orchestrate/backfill', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ startDate, endDate })
                });
                
                const result = await response.json();
                
                if (response.ok) {
                    resultDiv.textContent = '✅ Backfill Orchestration Success:\\n\\n' + JSON.stringify(result, null, 2);
                    resultDiv.style.background = '#d4edda';
                } else {
                    resultDiv.textContent = '❌ Backfill Orchestration Error:\\n\\n' + JSON.stringify(result, null, 2);
                    resultDiv.style.background = '#f8d7da';
                }
            } catch (error) {
                resultDiv.textContent = '❌ Network Error: ' + error.message;
                resultDiv.style.background = '#f8d7da';
            }
        }
    </script>
</body>
</html>
  `);
});

// Health check endpoint
app.get('/', (req, res) => {
  res.json({
    status: 'healthy',
    service: 'SMG Data Orchestrator',
    version: '1.5.0',
    endpoints: {
      '/orchestrate': 'POST - Run complete SMG data flow',
      '/orchestrate/backfill': 'POST - Backfill date range',
      '/orchestrate/daily': 'POST - Run daily data scraping and processing',
      '/status': 'GET - Service status and health checks',
      '/test': 'GET - Test page for browser testing'
    },
    services: {
      scraper: SCRAPER_URL,
      pipeline: PIPELINE_URL
    },
    fixes_applied: [
      'Now uses correct scraper endpoints: /smg-download and GET /smg-backfill',
      'Fixed 404 error by removing self-referencing HTTP calls',
      'CRITICAL FIX: Uses real CSV data from scraper instead of hardcoded test data',
      'FIELD NAME FIX: Added support for csvContent field from scraper response'
    ],
    timestamp: new Date().toISOString()
  });
});

// ENHANCED CSV DATA EXTRACTION FUNCTION WITH csvContent FIELD SUPPORT
async function extractCSVDataFromScraper(scrapingResults, mode, dateParam = null) {
  console.log('🔍 DEBUG: extractCSVDataFromScraper() - START');
  console.log('📊 DEBUG: scrapingResults type:', typeof scrapingResults);
  console.log('📊 DEBUG: scrapingResults keys:', scrapingResults ? Object.keys(scrapingResults) : 'NULL');
  console.log('📊 DEBUG: mode:', mode);
  console.log('📊 DEBUG: dateParam:', dateParam);
  
  // Log full scraping results for analysis
  console.log('📊 DEBUG: Full scrapingResults:', JSON.stringify(scrapingResults, null, 2));
  
  // Method 1: Check if CSV data is directly provided (from /smg-daily endpoint)
  console.log('🔍 DEBUG: Checking Method 1 - Direct CSV data');
  if (scrapingResults.csv_data) {
    console.log('📊 DEBUG: Found csv_data field, length:', scrapingResults.csv_data.length);
    console.log('📊 DEBUG: csv_data content preview:', scrapingResults.csv_data.substring(0, 200));
    
    if (scrapingResults.csv_data.length > 100) {
      console.log('✅ DEBUG: Method 1 SUCCESS - Using direct CSV data');
      return scrapingResults.csv_data;
    } else {
      console.log('⚠️ DEBUG: Method 1 REJECTED - CSV data too short');
    }
  } else {
    console.log('📊 DEBUG: Method 1 SKIPPED - No csv_data field found');
  }
  
  // Method 1b: Check for csvContent in result object (FIX for scraper field name mismatch)
  console.log('🔍 DEBUG: Checking Method 1b - csvContent in result');
  if (scrapingResults.result && scrapingResults.result.csvContent) {
    console.log('📊 DEBUG: Found result.csvContent field, length:', scrapingResults.result.csvContent.length);
    console.log('📊 DEBUG: csvContent preview:', scrapingResults.result.csvContent.substring(0, 200));
    
    if (scrapingResults.result.csvContent.length > 100) {
      console.log('✅ DEBUG: Method 1b SUCCESS - Using csvContent from result');
      return scrapingResults.result.csvContent;
    } else {
      console.log('⚠️ DEBUG: Method 1b REJECTED - csvContent too short');
    }
  } else {
    console.log('📊 DEBUG: Method 1b SKIPPED - No result.csvContent field found');
  }
  
  // Method 2: Try to get download path and read file (from /smg-download endpoint)
  console.log('🔍 DEBUG: Checking Method 2 - File download path');
  if (scrapingResults.result && scrapingResults.result.downloadPath) {
    console.log('📊 DEBUG: Found downloadPath:', scrapingResults.result.downloadPath);
    try {
      const fs = require('fs');
      const csvData = fs.readFileSync(scrapingResults.result.downloadPath, 'utf8');
      console.log(`✅ DEBUG: Method 2 SUCCESS - Read ${csvData.length} characters from file`);
      console.log('📊 DEBUG: File content preview:', csvData.substring(0, 200));
      return csvData;
    } catch (fileError) {
      console.log(`❌ DEBUG: Method 2 FAILED - Could not read file: ${fileError.message}`);
    }
  } else {
    console.log('📊 DEBUG: Method 2 SKIPPED - No downloadPath found');
    if (scrapingResults.result) {
      console.log('📊 DEBUG: result object keys:', Object.keys(scrapingResults.result));
    }
  }
  
  // Method 2b: Check for other possible file content fields
  console.log('🔍 DEBUG: Checking Method 2b - Alternative content fields');
  const possibleContentFields = ['content', 'file_content', 'data', 'csv_content', 'csvContent', 'body'];
  for (const field of possibleContentFields) {
    if (scrapingResults[field] || (scrapingResults.result && scrapingResults.result[field])) {
      const content = scrapingResults[field] || scrapingResults.result[field];
      console.log(`📊 DEBUG: Found ${field} field, length:`, content.length);
      console.log(`📊 DEBUG: ${field} preview:`, content.substring(0, 200));
      
      if (content.length > 100) {
        console.log(`✅ DEBUG: Method 2b SUCCESS - Using ${field} data`);
        return content;
      }
    }
  }
  
  // Method 3: Try alternative scraper endpoint for better data extraction
  console.log('🔍 DEBUG: Checking Method 3 - Alternative endpoint fallback');
  if (mode === 'daily' && dateParam) {
    console.log('📊 DEBUG: Attempting fallback to /smg-daily endpoint...');
    try {
      const response = await axios.post(`${SCRAPER_URL}/smg-daily`, {}, {
        timeout: 300000
      });
      
      console.log('📊 DEBUG: Fallback response status:', response.status);
      console.log('📊 DEBUG: Fallback response data keys:', Object.keys(response.data));
      
      if (response.data && response.data.csv_data && response.data.csv_data.length > 100) {
        console.log('✅ DEBUG: Method 3 SUCCESS - Got CSV data from /smg-daily fallback');
        console.log('📊 DEBUG: Fallback csv_data length:', response.data.csv_data.length);
        console.log('📊 DEBUG: Fallback csv_data preview:', response.data.csv_data.substring(0, 200));
        return response.data.csv_data;
      } else {
        console.log('❌ DEBUG: Method 3 FAILED - Fallback endpoint returned insufficient data');
      }
    } catch (fallbackError) {
      console.log(`❌ DEBUG: Method 3 FAILED - Fallback endpoint error: ${fallbackError.message}`);
    }
  } else {
    console.log('📊 DEBUG: Method 3 SKIPPED - Not daily mode or no dateParam');
  }
  
  // Method 4: Check if scraper response contains file content directly
  console.log('🔍 DEBUG: Checking Method 4 - Direct response analysis');
  if (typeof scrapingResults === 'string' && scrapingResults.length > 100) {
    console.log('✅ DEBUG: Method 4 SUCCESS - Response is a string, using as CSV');
    console.log('📊 DEBUG: String response length:', scrapingResults.length);
    console.log('📊 DEBUG: String response preview:', scrapingResults.substring(0, 200));
    return scrapingResults;
  }
  
  // Final fallback - generate error instead of using mock data
  console.log('❌ DEBUG: ALL METHODS FAILED - Could not extract real CSV data from any source');
  console.log('📊 DEBUG: Final scrapingResults analysis:');
  console.log('  - Type:', typeof scrapingResults);
  console.log('  - Keys:', scrapingResults ? Object.keys(scrapingResults) : 'NULL');
  console.log('  - Size:', JSON.stringify(scrapingResults).length, 'characters');
  
  throw new Error('Unable to extract CSV data from scraper response. No valid data found in response.');
}

// CORE ORCHESTRATION FUNCTION - UPDATED TO USE REAL DATA WITH DEBUG LOGGING
async function runOrchestration(mode, dates = null) {
  const orchestrationStart = new Date();
  const orchestrationId = `orch_${Date.now()}`;
  
  let orchestrationResults = {
    orchestration_id: orchestrationId,
    status: 'running',
    started_at: orchestrationStart.toISOString(),
    phases: {
      scraping: { status: 'pending', duration_ms: 0 },
      processing: { status: 'pending', duration_ms: 0 }
    },
    total_duration_ms: 0,
    records_processed: 0,
    errors: []
  };

  try {
    console.log(`🚀 Starting SMG orchestration ${orchestrationId} with csvContent field support...`);
    
    // PHASE 1: SCRAPING
    console.log('📥 Phase 1: Starting SMG data scraping...');
    const scrapingStart = Date.now();
    
    let scrapingResults;
    let csvData;
    try {
      if (mode === 'daily') {
        // Use /smg-download endpoint for yesterday's data
        const yesterday = new Date();
        yesterday.setDate(yesterday.getDate() - 1);
        const dateParam = yesterday.toISOString().split('T')[0]; // YYYY-MM-DD format
        
        console.log(`📅 Scraping daily data for: ${dateParam}`);
        const response = await axios.get(`${SCRAPER_URL}/smg-download?date=${dateParam}`, {
          timeout: 300000 // 5 minute timeout for scraping
        });
        scrapingResults = response.data;
        
        console.log('📊 DEBUG: Scraper response status:', response.status);
        console.log('📊 DEBUG: Scraper response size:', JSON.stringify(scrapingResults).length, 'characters');
        
        // FIXED: Extract real CSV data instead of using mock data
        csvData = await extractCSVDataFromScraper(scrapingResults, 'daily', dateParam);
        
        // Convert single download result to expected format
        if (scrapingResults.status === 'success' || scrapingResults.result) {
          scrapingResults.files_processed = 1;
          scrapingResults.dates_processed = [dateParam];
          scrapingResults.csv_data = csvData;
        }
        
      } else if (mode === 'backfill' && dates) {
        // Use /smg-backfill endpoint with query parameters
        const startDate = dates[0];
        const endDate = dates[dates.length - 1];
        
        console.log(`📊 Scraping backfill data from: ${startDate} to ${endDate}`);
        const response = await axios.get(`${SCRAPER_URL}/smg-backfill?start=${startDate}&end=${endDate}`, {
          timeout: 600000 // 10 minute timeout for backfill
        });
        scrapingResults = response.data;
        
        console.log('📊 DEBUG: Backfill scraper response status:', response.status);
        console.log('📊 DEBUG: Backfill scraper response size:', JSON.stringify(scrapingResults).length, 'characters');
        
        // Extract real CSV data for backfill
        csvData = await extractCSVDataFromScraper(scrapingResults, 'backfill');
        
        // Convert backfill result to expected format
        if (scrapingResults.status === 'success') {
          scrapingResults.files_processed = scrapingResults.summary?.successCount || scrapingResults.files_processed || 0;
          scrapingResults.dates_processed = dates;
          scrapingResults.csv_data = csvData;
        }
        
      } else {
        throw new Error('Invalid mode or missing dates for backfill');
      }
      
      // Validate we have real CSV data
      if (!csvData || csvData.length < 100) {
        throw new Error(`Scraper returned insufficient CSV data - possible scraping failure. Got ${csvData ? csvData.length : 0} characters.`);
      }
      
      // Count CSV records for validation
      const csvLines = csvData.split('\n').filter(line => line.trim().length > 0);
      const csvRecords = csvLines.length - 1; // Subtract header
      
      console.log(`📊 CSV data extracted: ${csvData.length} characters, ${csvRecords} data records`);
      console.log(`📊 DEBUG: CSV first 200 characters: ${csvData.substring(0, 200)}`);
      
      orchestrationResults.phases.scraping = {
        status: 'completed',
        duration_ms: Date.now() - scrapingStart,
        files_scraped: scrapingResults.files_processed || 0,
        dates_processed: scrapingResults.dates_processed || [],
        csv_records_extracted: csvRecords,
        csv_data_size: csvData.length,
        scraper_endpoint_used: mode === 'daily' ? '/smg-download' : '/smg-backfill'
      };
      
      console.log(`✅ Phase 1 complete: ${scrapingResults.files_processed || 0} files scraped, ${csvRecords} records extracted`);
      
    } catch (error) {
      orchestrationResults.phases.scraping = {
        status: 'failed',
        duration_ms: Date.now() - scrapingStart,
        error: error.message,
        scraper_url_attempted: mode === 'daily' ? `${SCRAPER_URL}/smg-download` : `${SCRAPER_URL}/smg-backfill`
      };
      orchestrationResults.errors.push(`Scraping failed: ${error.message}`);
      throw error;
    }
    
    // PHASE 2: PROCESSING
    console.log('🔄 Phase 2: Starting data processing...');
    const processingStart = Date.now();
    
    try {
      // Check if we have CSV data to process
      if (!csvData || csvData.length === 0) {
        throw new Error('No CSV data available for processing');
      }
      
      console.log(`📤 Sending ${csvData.length} characters of CSV data to pipeline...`);
      console.log(`📊 DEBUG: CSV data preview being sent: ${csvData.substring(0, 200)}`);
      
      // Process the scraped data through the pipeline
      const response = await axios.post(`${PIPELINE_URL}/smg-pipeline`, {
        csvData: csvData,
        uploadMode: mode === 'daily' ? 'replace' : 'upsert'
      }, {
        timeout: 120000 // 2 minute timeout for processing
      });
      
      const processingResults = response.data;
      
      orchestrationResults.phases.processing = {
        status: 'completed',
        duration_ms: Date.now() - processingStart,
        records_processed: processingResults.pipeline_results?.records_processed || 0,
        upload_mode: processingResults.pipeline_results?.stages?.upload?.upload_mode,
        csv_input_size: csvData.length
      };
      
      orchestrationResults.records_processed = processingResults.pipeline_results?.records_processed || 0;
      
      console.log(`✅ Phase 2 complete: ${orchestrationResults.records_processed} records processed`);
      
    } catch (error) {
      orchestrationResults.phases.processing = {
        status: 'failed',
        duration_ms: Date.now() - processingStart,
        error: error.message
      };
      orchestrationResults.errors.push(`Processing failed: ${error.message}`);
      throw error;
    }
    
    // ORCHESTRATION COMPLETION
    orchestrationResults.status = 'completed';
    orchestrationResults.completed_at = new Date().toISOString();
    orchestrationResults.total_duration_ms = Date.now() - orchestrationStart.getTime();
    
    console.log(`🎉 SMG Orchestration complete: ${orchestrationResults.records_processed} records in ${orchestrationResults.total_duration_ms}ms`);
    
    return {
      success: true,
      orchestration: orchestrationResults,
      timestamp: new Date().toISOString()
    };
    
  } catch (error) {
    console.error('❌ SMG orchestration error:', error);
    
    orchestrationResults.status = 'failed';
    orchestrationResults.completed_at = new Date().toISOString();
    orchestrationResults.total_duration_ms = Date.now() - orchestrationStart.getTime();
    orchestrationResults.final_error = error.message;
    
    throw {
      success: false,
      orchestration: orchestrationResults,
      error: 'Orchestration failed',
      message: error.message,
      timestamp: new Date().toISOString()
    };
  }
}

// MAIN ORCHESTRATION ENDPOINT
app.post('/orchestrate', async (req, res) => {
  try {
    const { dates, mode = 'daily' } = req.body;
    const result = await runOrchestration(mode, dates);
    res.json(result);
  } catch (error) {
    res.status(500).json(error);
  }
});

// DAILY ORCHESTRATION ENDPOINT - FIXED VERSION
app.post('/orchestrate/daily', async (req, res) => {
  console.log('📅 Daily SMG orchestration triggered...');
  
  try {
    // Call core orchestration function directly instead of HTTP call
    const result = await runOrchestration('daily');
    
    res.json({
      success: true,
      message: 'Daily orchestration completed',
      results: result,
      timestamp: new Date().toISOString()
    });
    
  } catch (error) {
    console.error('❌ Daily orchestration error:', error);
    res.status(500).json({
      success: false,
      error: 'Daily orchestration failed',
      message: error.message || (error.orchestration ? error.orchestration.final_error : 'Unknown error'),
      orchestration_details: error.orchestration || null,
      timestamp: new Date().toISOString()
    });
  }
});

// BACKFILL ORCHESTRATION ENDPOINT - FIXED VERSION
app.post('/orchestrate/backfill', async (req, res) => {
  console.log('📊 Backfill SMG orchestration triggered...');
  
  const { startDate, endDate, dates } = req.body;
  
  if (!dates && (!startDate || !endDate)) {
    return res.status(400).json({
      error: 'Missing required parameters',
      message: 'Provide either dates array or startDate/endDate range',
      timestamp: new Date().toISOString()
    });
  }
  
  try {
    // Generate date array if range provided
    let targetDates = dates;
    if (!targetDates && startDate && endDate) {
      targetDates = generateDateRange(startDate, endDate);
    }
    
    // Call core orchestration function directly instead of HTTP call
    const result = await runOrchestration('backfill', targetDates);
    
    res.json({
      success: true,
      message: `Backfill orchestration completed for ${targetDates.length} dates`,
      dates_processed: targetDates,
      results: result,
      timestamp: new Date().toISOString()
    });
    
  } catch (error) {
    console.error('❌ Backfill orchestration error:', error);
    res.status(500).json({
      success: false,
      error: 'Backfill orchestration failed',
      message: error.message || (error.orchestration ? error.orchestration.final_error : 'Unknown error'),
      orchestration_details: error.orchestration || null,
      timestamp: new Date().toISOString()
    });
  }
});

// STATUS ENDPOINT
app.get('/status', async (req, res) => {
  try {
    console.log('📊 Checking orchestrator system status...');
    const statusStart = Date.now();
    
    const systemStatus = {
      overall_status: 'healthy',
      timestamp: new Date().toISOString(),
      uptime_seconds: process.uptime(),
      service_health: {},
      configuration: {
        scraper_url: SCRAPER_URL,
        pipeline_url: PIPELINE_URL,
        node_version: process.version,
        environment: process.env.NODE_ENV || 'production',
        fixes_applied: [
          'Using correct scraper endpoints: /smg-download and GET /smg-backfill',
          'Fixed 404 error by removing self-referencing HTTP calls',
          'CRITICAL FIX: Now uses real CSV data from scraper instead of hardcoded test data',
          'FIELD NAME FIX: Added support for csvContent field from scraper response'
        ]
      },
      scheduled_jobs: {
        daily_automation: {
          enabled: false, // Will be enabled when cron is set up
          schedule: '30 12 * * *', // 8:30 AM ET (12:30 UTC)
          next_run: 'Not scheduled'
        }
      }
    };
    
    // Test connectivity to both services
    try {
      const [scraperHealth, pipelineHealth] = await Promise.allSettled([
        axios.get(`${SCRAPER_URL}/`, { timeout: 10000 }),
        axios.get(`${PIPELINE_URL}/`, { timeout: 10000 })
      ]);
      
      systemStatus.service_health.scraper = {
        status: scraperHealth.status === 'fulfilled' ? 'healthy' : 'unhealthy',
        url: SCRAPER_URL,
        response_code: scraperHealth.status === 'fulfilled' ? scraperHealth.value.status : null,
        error: scraperHealth.status === 'rejected' ? scraperHealth.reason.message : null,
        available_endpoints: ['/smg-download?date=YYYY-MM-DD', '/smg-daily (POST)', '/smg-backfill?start=YYYY-MM-DD&end=YYYY-MM-DD']
      };
      
      systemStatus.service_health.pipeline = {
        status: pipelineHealth.status === 'fulfilled' ? 'healthy' : 'unhealthy',
        url: PIPELINE_URL,
        response_code: pipelineHealth.status === 'fulfilled' ? pipelineHealth.value.status : null,
        error: pipelineHealth.status === 'rejected' ? pipelineHealth.reason.message : null,
        available_endpoints: ['/smg-pipeline', '/smg-status', '/test']
      };
      
      // Determine overall status
      if (systemStatus.service_health.scraper.status === 'unhealthy' || 
          systemStatus.service_health.pipeline.status === 'unhealthy') {
        systemStatus.overall_status = 'degraded';
      }
      
    } catch (error) {
      systemStatus.overall_status = 'critical';
      systemStatus.service_health.error = error.message;
    }
    
    systemStatus.status_check_duration_ms = Date.now() - statusStart;
    
    console.log(`✅ Status check complete: ${systemStatus.overall_status}`);
    res.json(systemStatus);
    
  } catch (error) {
    console.error('❌ Status check error:', error);
    res.status(500).json({
      overall_status: 'critical',
      error: 'Status check failed',
      message: error.message,
      timestamp: new Date().toISOString()
    });
  }
});

// UTILITY FUNCTIONS
function generateDateRange(startDate, endDate) {
  const dates = [];
  const start = new Date(startDate);
  const end = new Date(endDate);
  
  for (let date = new Date(start); date <= end; date.setDate(date.getDate() + 1)) {
    dates.push(date.toISOString().split('T')[0]);
  }
  
  return dates;
}

// SCHEDULED AUTOMATION (8:30 AM ET = 12:30 UTC)
// Commented out initially - will enable after testing
/*
cron.schedule('30 12 * * *', async () => {
  console.log('⏰ Scheduled daily SMG orchestration starting...');
  
  try {
    const result = await runOrchestration('daily');
    console.log('✅ Scheduled orchestration completed:', result);
  } catch (error) {
    console.error('❌ Scheduled orchestration failed:', error.message);
  }
}, {
  timezone: 'America/New_York'
});
*/

// Start server
app.listen(PORT, () => {
  console.log(`🚀 SMG Orchestrator running on port ${PORT}`);
  console.log('🔍 FIELD NAME FIX: Now supports csvContent field from scraper response');
  console.log('Service Configuration:');
  console.log('- Scraper URL:', SCRAPER_URL);
  console.log('- Pipeline URL:', PIPELINE_URL);
  console.log('\nFixes Applied:');
  console.log('- Endpoint fixes: Daily mode uses GET /smg-download?date=YYYY-MM-DD');
  console.log('- Endpoint fixes: Backfill mode uses GET /smg-backfill?start=YYYY-MM-DD&end=YYYY-MM-DD');
  console.log('- Internal call fix: Removed self-referencing HTTP calls to prevent 404 errors');
  console.log('- CRITICAL FIX: Now extracts and uses real CSV data from scraper instead of hardcoded test data');
  console.log('- FIELD NAME FIX: Added support for csvContent field from scraper response (Method 1b)');
  console.log('\nAvailable Endpoints:');
  console.log('- GET  /           - Health check');
  console.log('- GET  /test       - Browser test page');
  console.log('- POST /orchestrate - Main orchestration');
  console.log('- POST /orchestrate/daily - Daily automation');
  console.log('- POST /orchestrate/backfill - Backfill date ranges');
  console.log('- GET  /status     - System status');
});

module.exports = app;