@echo off
echo Starting Data Cleanup...

echo Deleting checkpoints (Spark State)...
if exist "data\checkpoints" rmdir /s /q "data\checkpoints"

echo Deleting data lake (Parquet files)...
if exist "data\lake" rmdir /s /q "data\lake"

echo Deleting reports (Airflow outputs)...
if exist "data\reports" rmdir /s /q "data\reports"

echo.
echo Recreating empty folders...
mkdir "data\checkpoints"
mkdir "data\lake"
mkdir "data\reports"
mkdir "data\lake\transactions" 

echo.
echo Clean up complete
pause