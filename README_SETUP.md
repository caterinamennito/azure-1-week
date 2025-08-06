# Azure Functions Train Data Pipeline

## 🎯 Overview
Azure Functions app that fetches Belgian train data from iRail API and stores it in Azure SQL Database with automated data collection every 15 minutes.

## 🛠️ Tech Stack
- **Azure Functions** (Python 3.11)
- **Azure SQL Database** 
- **iRail API** (Belgian railway data)
- **pandas** for data processing
- **Docker** for local development

## 🚀 Development Journey

### 1. Initial Database Connection Attempts
- **First tried**: Direct ODBC connection locally
- **Issue**: Complex setup and driver management on different OS
- **Switched to**: Docker for consistent local development environment

### 2. Local Development with Docker
- **Why Docker**: Consistent Azure Functions runtime across environments
- **Setup**: `docker-compose.yml` with Azure Functions host
- **Benefit**: Identical local/cloud environment

### 3. Function Architecture Issues
- **First tried**: Module-level database initialization
- **Issue**: 401 Unauthorized errors in Azure Functions
- **Solution**: Moved initialization inside function calls (Azure Functions constraint)

### 4. Azure Deployment Challenges
- **Environment Variables**: Had to manually add `SQL_CONNECTION_STRING` in Azure Portal
- **ODBC Driver**: Changed from "Driver 18" to "Driver 17" for Linux compatibility
- **Deployment**: Used VS Code Azure Functions extension

## 📁 Key Files
```
├── function_app.py          # Main functions
├── data_validator.py        # Data validation logic  
├── docker-compose.yml       # Local development
├── requirements.txt         # Dependencies
└── host.json               # Azure Functions config
```

## 🧪 Functions Deployed
- **HTTP**: `/api/trains?station=<station-name` - On-demand data fetch
- **Timer**: Every 15 minutes - Automated collection for 5 major stations

## 🎯 Key Decisions Made

1. **Docker over local setup** → Consistent development environment
2. **Lazy initialization** → Avoid Azure Functions cold start issues  
3. **pandas for validation** → Efficient data manipulation and deduplication
4. **Bulk inserts** → Better database performance
5. **ODBC Driver 17** → Linux compatibility in Azure

## 📊 Current Status
✅ Deployed and running on Azure  
✅ Collecting data every 15 minutes  
✅ Processing ~40-50 train records per station  
✅ Robust error handling and validation

## 🔗 Live Endpoint
```bash
curl "https://trains-app--ena9gfemexchhqhz.francecentral-01.azurewebsites.net/api/trains?station=Brussels-Central"
```