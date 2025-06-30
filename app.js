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

// Configuration for loop strategy
const LOOP_CONFIG = {
  daily_lookback_days: 3, // Process last 3 days in daily mode
  max_parallel_dates: 1,  // Process dates sequentially for reliability
  retry_failed_dates: false // Keep it simple - no retries initially
};

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
    <div class="status success">✅ Orchestrator running with LOOP STRATEGY!</div>
    <div class="status warning">🔄 NEW: Simple loop iteration around proven working daily logic</div>
    
    <div class="module">
        <h3>📊 System Status Check</h3>
        <p>Test connectivity to all services (Scraper + Pipeline + Orchestrator):</p>
        <button onclick="testStatus()">🔍 Check System Status</button>
        <div id="statusResult" class="result">Click button to test...</div>
    </div>
    
    <div class="module">
        <h3>📅 Daily Orchestration Test (Loop Strategy)</h3>
        <p>Test complete daily flow processing last ${LOOP_CONFIG.daily_lookback_days} days using proven working logic:</p>
        <div class="warning">⚠️ This will process real data for multiple dates - each using working 1,650 record logic!</div>
        <button onclick="testDaily()">🔄 Test Daily Loop (${LOOP_CONFIG.daily_lookback_days} days)</button>
        <div id="dailyResult" class="result">Click button to test...</div>
    </div>
    
    <div class="module">
        <h3>📋 Backfill Test (Loop Strategy)</h3>
        <p>Test backfill orchestration with date iteration - each date processed independently:</p>
        <label>Start Date:</label> <input type="date" id="startDate" value="2025-06-17">
        <label>End Date:</label> <input type="date" id="endDate" value="2025-06-18">
        <br><br>
        <div class="warning">⚠️ Each date will be processed using proven working logic!</div>
        <button onclick="testBackfill()">📊 Test Backfill Loop</button>
        <div id="backfillResult" class="result">Click button to test...</div>
    </div>

    <div class="module">
        <h3>🔗 Service Links</h3>
        <p>
            <a href="/status" target="_blank">Orchestrator Status</a> | 
            <a href="${SCRAPER_URL}" target="_blank">Phase 1: Scraper</a> | 
            <a href="${PIPELINE_URL}" target="_blank">Phase 2: Pipeline</a>
        </p>
        <p><strong>Architecture:</strong> Phase 1 (Scraper) → Phase 3 (Orchestrator with Loops) → Phase 2 (Pipeline)</p>
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
            resultDiv.textContent = '🔄 Running daily loop orchestration...\\nThis may take several minutes per date...';
            resultDiv.className = 'result';
            
            try {
                const response = await fetch(baseUrl + '/orchestrate/daily', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' }
                });
                
                const result = await response.json();
                
                if (response.ok) {
                    resultDiv.textContent = '✅ Daily Loop Orchestration Success:\\n\\n' + JSON.stringify(result, null, 2);
                    resultDiv.style.background = '#d4edda';
                } else {
                    resultDiv.textContent = '❌ Daily Loop Orchestration Error:\\n\\n' + JSON.stringify(result, null, 2);
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
            
            resultDiv.textContent = `📊 Running backfill loop orchestration for ${startDate} to ${endDate}...\\nThis may take several minutes per date...`;
            resultDiv.className = 'result';
            
            try {
                const response = await fetch(baseUrl + '/orchestrate/backfill', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ startDate, endDate })
                });
                
                const result = await response.json();
                
                if (response.ok) {
                    resultDiv.textContent = '✅ Backfill Loop Orchestration Success:\\n\\n' + JSON.stringify(result, null, 2);
                    resultDiv.style.background = '#d4edda';
                } else {
                    resultDiv.textContent = '❌ Backfill Loop Orchestration Error:\\n\\n' + JSON.stringify(result, null, 2);
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
    service: 'SMG Data Orchestrator - Loop Strategy',
    version: '2.0.0',
    strategy: 'Simple loop iteration around proven working daily logic',
    endpoints: {
      '/orchestrate': 'POST - Run complete SMG data flow',
      '/orchestrate/backfill': 'POST - Backfill date range with loop iteration',
      '/orchestrate/daily': 'POST - Run daily data scraping for last 3 days with loop iteration',
      '/status': 'GET - Service status and health checks',
      '/test': 'GET - Test page for browser testing'
    },
    services: {
      scraper: SCRAPER_URL,
      pipeline: PIPELINE_URL
    },
    loop_configuration: LOOP_CONFIG,
    fixes_applied: [
      'REVERTED to proven working Method 1b (result.csvContent) from commit a43b0d3a',
      'ADDED simple loop wrapper around working daily orchestration logic',
      'MAINTAINED all working CSV extraction that processes 1,650 records successfully',
      'ENHANCED with date iteration for both daily (last 3 days) and backfill modes',
      'ISOLATED each date processing for better debugging and fail-safe operation'
    ],
    timestamp: new Date().toISOString()
  });
});

// ENHANCED CSV DATA EXTRACTION FUNCTION - REVERTED TO WORKING VERSION
async function extractCSVDataFromScraper(scrapingResults, mode, dateParam = null) {
  console.log('🔍 DEBUG: extractCSVDataFromScraper() - START');
  console.log('📊 DEBUG: scrapingResults type:', typeof scrapingResults);
  console.log('📊 DEBUG: scrapingResults keys:', scrapingResults ? Object.keys(scrapingResults) : 'NULL');
  console.log('📊 DEBUG: mode:', mode);
  console.log('📊 DEBUG: dateParam:', dateParam);
  
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
  
  // Method 1b: Check for csvContent in result object (PROVEN WORKING FIX)
  console.log('🔍 DEBUG: Checking Method 1b - csvContent in result (PROVEN WORKING)');
  if (scrapingResults.result && scrapingResults.result.csvContent) {
    console.log('📊 DEBUG: Found result.csvContent field, length:', scrapingResults.result.csvContent.length);
    console.log('📊 DEBUG: csvContent preview:', scrapingResults.result.csvContent.substring(0, 200));
    
    if (scrapingResults.result.csvContent.length > 100) {
      console.log('✅ DEBUG: Method 1b SUCCESS - Using csvContent from result (PROVEN WORKING)');
      return scrapingResults.result.csvContent;
    } else {
      console.log('⚠️ DEBUG: Method 1b REJECTED - csvContent too short');
    }
  } else {
    console.log('📊 DEBUG: Method 1b SKIPPED - No result.csvContent field found');
  }
  
  // Method 2: Try to get download path and read file
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
  }
  
  // Final fallback - generate error instead of using mock data
  console.log('❌ DEBUG: ALL METHODS FAILED - Could not extract real CSV data from any source');
  console.log('📊 DEBUG: Final scrapingResults analysis:');
  console.log('  - Type:', typeof scrapingResults);
  console.log('  - Keys:', scrapingResults ? Object.keys(scrapingResults) : 'NULL');
  console.log('  - Size:', JSON.stringify(scrapingResults).length, 'characters');
  
  throw new Error('Unable to extract CSV data from scraper response. No valid data found in response.');
}

// SINGLE DATE ORCHESTRATION FUNCTION - REVERTED TO PROVEN WORKING VERSION
async function runSingleDateOrchestration(targetDate) {
  const orchestrationStart = new Date();
  const orchestrationId = `single_${targetDate}_${Date.now()}`;
  
  let orchestrationResults = {
    orchestration_id: orchestrationId,
    target_date: targetDate,
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
    console.log(`🚀 Starting single date SMG orchestration ${orchestrationId} for ${targetDate}...`);
    
    // PHASE 1: SCRAPING
    console.log(`📥 Phase 1: Starting SMG data scraping for ${targetDate}...`);
    const scrapingStart = Date.now();
    
    let scrapingResults;
    let csvData;
    try {
      console.log(`📅 Scraping daily data for: ${targetDate}`);
      const response = await axios.get(`${SCRAPER_URL}/smg-download?date=${targetDate}`, {
        timeout: 300000 // 5 minute timeout for scraping
      });
      scrapingResults = response.data;
      
      console.log('📊 DEBUG: Scraper response status:', response.status);
      console.log('📊 DEBUG: Scraper response size:', JSON.stringify(scrapingResults).length, 'characters');
      
      // PROVEN: Extract real CSV data using working Method 1b
      csvData = await extractCSVDataFromScraper(scrapingResults, 'daily', targetDate);
      
      // Convert single download result to expected format
      if (scrapingResults.status === 'success' || scrapingResults.result) {
        scrapingResults.files_processed = 1;
        scrapingResults.dates_processed = [targetDate];
        scrapingResults.csv_data = csvData;
      }
      
      // Validate we have real CSV data
      if (!csvData || csvData.length < 100) {
        throw new Error(`Scraper returned insufficient CSV data for ${targetDate} - possible scraping failure. Got ${csvData ? csvData.length : 0} characters.`);
      }
      
      // Count CSV records for validation
      const csvLines = csvData.split('\n').filter(line => line.trim().length > 0);
      const csvRecords = csvLines.length - 1; // Subtract header
      
      console.log(`📊 CSV data extracted for ${targetDate}: ${csvData.length} characters, ${csvRecords} data records`);
      
      orchestrationResults.phases.scraping = {
        status: 'completed',
        duration_ms: Date.now() - scrapingStart,
        files_scraped: scrapingResults.files_processed || 0,
        dates_processed: scrapingResults.dates_processed || [],
        csv_records_extracted: csvRecords,
        csv_data_size: csvData.length,
        scraper_endpoint_used: `/smg-download?date=${targetDate}`
      };
      
      console.log(`✅ Phase 1 complete for ${targetDate}: ${scrapingResults.files_processed || 0} files scraped, ${csvRecords} records extracted`);
      
    } catch (error) {
      orchestrationResults.phases.scraping = {
        status: 'failed',
        duration_ms: Date.now() - scrapingStart,
        error: error.message,
        scraper_url_attempted: `${SCRAPER_URL}/smg-download?date=${targetDate}`
      };
      orchestrationResults.errors.push(`Scraping failed for ${targetDate}: ${error.message}`);
      throw error;
    }
    
    // PHASE 2: PROCESSING
    console.log(`🔄 Phase 2: Starting data processing for ${targetDate}...`);
    const processingStart = Date.now();
    
    try {
      // Check if we have CSV data to process
      if (!csvData || csvData.length === 0) {
        throw new Error(`No CSV data available for processing ${targetDate}`);
      }
      
      console.log(`📤 Sending ${csvData.length} characters of CSV data to pipeline for ${targetDate}...`);
      
      // Process the scraped data through the pipeline
      const response = await axios.post(`${PIPELINE_URL}/smg-pipeline`, {
        csvData: csvData,
        uploadMode: 'upsert' // Use upsert for single date processing
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
      
      console.log(`✅ Phase 2 complete for ${targetDate}: ${orchestrationResults.records_processed} records processed`);
      
    } catch (error) {
      orchestrationResults.phases.processing = {
        status: 'failed',
        duration_ms: Date.now() - processingStart,
        error: error.message
      };
      orchestrationResults.errors.push(`Processing failed for ${targetDate}: ${error.message}`);
      throw error;
    }
    
    // ORCHESTRATION COMPLETION
    orchestrationResults.status = 'completed';
    orchestrationResults.completed_at = new Date().toISOString();
    orchestrationResults.total_duration_ms = Date.now() - orchestrationStart.getTime();
    
    console.log(`🎉 SMG Orchestration complete for ${targetDate}: ${orchestrationResults.records_processed} records in ${orchestrationResults.total_duration_ms}ms`);
    
    return {
      success: true,
      date: targetDate,
      orchestration: orchestrationResults,
      timestamp: new Date().toISOString()
    };
    
  } catch (error) {
    console.error(`❌ SMG orchestration error for ${targetDate}:`, error);
    
    orchestrationResults.status = 'failed';
    orchestrationResults.completed_at = new Date().toISOString();
    orchestrationResults.total_duration_ms = Date.now() - orchestrationStart.getTime();
    orchestrationResults.final_error = error.message;
    
    return {
      success: false,
      date: targetDate,
      orchestration: orchestrationResults,
      error: 'Orchestration failed',
      message: error.message,
      timestamp: new Date().toISOString()
    };
  }
}

// ENHANCED LOOP ORCHESTRATION FUNCTION - NEW SIMPLE STRATEGY
async function runLoopOrchestration(mode, dates = null) {
  const loopStart = new Date();
  const loopId = `loop_${mode}_${Date.now()}`;
  
  let targetDates = [];
  
  // Generate target dates based on mode
  if (mode === 'daily') {
    // Process last N days (configurable)
    const today = new Date();
    for (let i = 1; i <= LOOP_CONFIG.daily_lookback_days; i++) {
      const date = new Date(today);
      date.setDate(date.getDate() - i);
      targetDates.push(date.toISOString().split('T')[0]);
    }
    console.log(`📅 Daily mode: Processing last ${LOOP_CONFIG.daily_lookback_days} days: ${targetDates.join(', ')}`);
  } else if (mode === 'backfill' && dates) {
    targetDates = Array.isArray(dates) ? dates : [dates];
    console.log(`📊 Backfill mode: Processing ${targetDates.length} dates: ${targetDates.join(', ')}`);
  } else {
    throw new Error('Invalid mode or missing dates for backfill');
  }
  
  let loopResults = {
    loop_id: loopId,
    mode: mode,
    target_dates: targetDates,
    status: 'running',
    started_at: loopStart.toISOString(),
    date_results: [],
    summary: {
      total_dates: targetDates.length,
      successful_dates: 0,
      failed_dates: 0,
      total_records_processed: 0,
      total_duration_ms: 0
    },
    errors: []
  };
  
  try {
    console.log(`🔄 Starting loop orchestration ${loopId} for ${targetDates.length} dates...`);
    
    // Process each date using proven working daily logic
    for (let i = 0; i < targetDates.length; i++) {
      const targetDate = targetDates[i];
      console.log(`\n🎯 Processing date ${i + 1}/${targetDates.length}: ${targetDate}`);
      
      try {
        // Use proven working single date orchestration
        const dateResult = await runSingleDateOrchestration(targetDate);
        
        loopResults.date_results.push(dateResult);
        
        if (dateResult.success) {
          loopResults.summary.successful_dates++;
          loopResults.summary.total_records_processed += dateResult.orchestration.records_processed || 0;
          console.log(`✅ Date ${targetDate} completed: ${dateResult.orchestration.records_processed} records`);
        } else {
          loopResults.summary.failed_dates++;
          loopResults.errors.push(`Date ${targetDate}: ${dateResult.message}`);
          console.log(`❌ Date ${targetDate} failed: ${dateResult.message}`);
        }
        
      } catch (dateError) {
        loopResults.summary.failed_dates++;
        loopResults.errors.push(`Date ${targetDate}: ${dateError.message}`);
        loopResults.date_results.push({
          success: false,
          date: targetDate,
          error: dateError.message,
          timestamp: new Date().toISOString()
        });
        console.log(`❌ Date ${targetDate} failed with exception: ${dateError.message}`);
      }
    }
    
    // LOOP COMPLETION
    loopResults.status = loopResults.summary.failed_dates === 0 ? 'completed' : 'completed_with_errors';
    loopResults.completed_at = new Date().toISOString();
    loopResults.summary.total_duration_ms = Date.now() - loopStart.getTime();
    
    console.log(`🎉 Loop orchestration complete: ${loopResults.summary.successful_dates}/${loopResults.summary.total_dates} dates successful, ${loopResults.summary.total_records_processed} total records processed`);
    
    return {
      success: loopResults.summary.failed_dates === 0,
      loop: loopResults,
      timestamp: new Date().toISOString()
    };
    
  } catch (error) {
    console.error('❌ Loop orchestration error:', error);
    
    loopResults.status = 'failed';
    loopResults.completed_at = new Date().toISOString();
    loopResults.summary.total_duration_ms = Date.now() - loopStart.getTime();
    loopResults.final_error = error.message;
    
    throw {
      success: false,
      loop: loopResults,
      error: 'Loop orchestration failed',
      message: error.message,
      timestamp: new Date().toISOString()
    };
  }
}

// MAIN ORCHESTRATION ENDPOINT - ENHANCED WITH LOOP STRATEGY
app.post('/orchestrate', async (req, res) => {
  try {
    const { dates, mode = 'daily' } = req.body;
    const result = await runLoopOrchestration(mode, dates);
    res.json(result);
  } catch (error) {
    res.status(500).json(error);
  }
});

// DAILY ORCHESTRATION ENDPOINT - ENHANCED WITH LOOP STRATEGY
app.post('/orchestrate/daily', async (req, res) => {
  console.log(`📅 Daily SMG orchestration triggered with loop strategy (${LOOP_CONFIG.daily_lookback_days} days)...`);
  
  try {
    // Call enhanced loop orchestration function
    const result = await runLoopOrchestration('daily');
    
    res.json({
      success: true,
      message: `Daily loop orchestration completed for ${LOOP_CONFIG.daily_lookback_days} days`,
      results: result,
      timestamp: new Date().toISOString()
    });
    
  } catch (error) {
    console.error('❌ Daily loop orchestration error:', error);
    res.status(500).json({
      success: false,
      error: 'Daily loop orchestration failed',
      message: error.message || (error.loop ? error.loop.final_error : 'Unknown error'),
      loop_details: error.loop || null,
      timestamp: new Date().toISOString()
    });
  }
});

// BACKFILL ORCHESTRATION ENDPOINT - ENHANCED WITH LOOP STRATEGY
app.post('/orchestrate/backfill', async (req, res) => {
  console.log('📊 Backfill SMG orchestration triggered with loop strategy...');
  
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
    
    // Call enhanced loop orchestration function
    const result = await runLoopOrchestration('backfill', targetDates);
    
    res.json({
      success: true,
      message: `Backfill loop orchestration completed for ${targetDates.length} dates`,
      dates_processed: targetDates,
      results: result,
      timestamp: new Date().toISOString()
    });
    
  } catch (error) {
    console.error('❌ Backfill loop orchestration error:', error);
    res.status(500).json({
      success: false,
      error: 'Backfill loop orchestration failed',
      message: error.message || (error.loop ? error.loop.final_error : 'Unknown error'),
      loop_details: error.loop || null,
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
        loop_strategy: LOOP_CONFIG,
        fixes_applied: [
          'REVERTED to proven working Method 1b (result.csvContent) from commit a43b0d3a',
          'ADDED simple loop wrapper around working daily orchestration logic',
          'ENHANCED with date iteration for reliable multi-date processing',
          'MAINTAINED all working CSV extraction that processes 1,650 records successfully',
          'ISOLATED each date processing for better debugging and fail-safe operation'
        ]
      },
      scheduled_jobs: {
        daily_automation: {
          enabled: false, // Will be enabled when cron is set up
          schedule: '30 12 * * *', // 8:30 AM ET (12:30 UTC)
          next_run: 'Not scheduled',
          loop_configuration: `Will process last ${LOOP_CONFIG.daily_lookback_days} days when enabled`
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

// SCHEDULED AUTOMATION (8:30 AM ET = 12:30 UTC) - Enhanced with loop strategy
// Commented out initially - will enable after testing
/*
cron.schedule('30 12 * * *', async () => {
  console.log(`⏰ Scheduled daily SMG loop orchestration starting (${LOOP_CONFIG.daily_lookback_days} days)...`);
  
  try {
    const result = await runLoopOrchestration('daily');
    console.log('✅ Scheduled loop orchestration completed:', result);
  } catch (error) {
    console.error('❌ Scheduled loop orchestration failed:', error.message);
  }
}, {
  timezone: 'America/New_York'
});
*/

// Start server
app.listen(PORT, () => {
  console.log(`🚀 SMG Orchestrator running on port ${PORT}`);
  console.log('🔄 LOOP STRATEGY: Simple iteration around proven working daily logic');
  console.log('Service Configuration:');
  console.log('- Scraper URL:', SCRAPER_URL);
  console.log('- Pipeline URL:', PIPELINE_URL);
  console.log('- Loop Configuration:', JSON.stringify(LOOP_CONFIG, null, 2));
  console.log('\nLoop Strategy:');
  console.log('- Foundation: Reverted to proven working Method 1b (result.csvContent)');
  console.log('- Daily mode: Process last', LOOP_CONFIG.daily_lookback_days, 'days using working logic');
  console.log('- Backfill mode: Iterate through date range using working logic');
  console.log('- Reliability: Each date processed independently with proven 1,650 record logic');
  console.log('\nAvailable Endpoints:');
  console.log('- GET  /           - Health check');
  console.log('- GET  /test       - Browser test page');
  console.log('- POST /orchestrate - Main loop orchestration');
  console.log('- POST /orchestrate/daily - Daily loop automation (last', LOOP_CONFIG.daily_lookback_days, 'days)');
  console.log('- POST /orchestrate/backfill - Backfill date ranges with loop iteration');
  console.log('- GET  /status     - System status');
});

module.exports = app;