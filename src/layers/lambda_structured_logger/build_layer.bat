@echo off
echo Building Lambda Structured Logger Layer...

REM Create build directory
if exist build rmdir /s /q build
mkdir build\lambda-structured-logger-layer

REM Copy the python package
xcopy /E /I python build\lambda-structured-logger-layer\python

REM Create the zip file
cd build
powershell -command "Compress-Archive -Path .\lambda-structured-logger-layer\* -DestinationPath ..\lambda-structured-logger-layer.zip -Force"
cd ..

echo Layer built successfully: lambda-structured-logger-layer.zip
echo Upload this zip file to AWS Lambda Layers

pause