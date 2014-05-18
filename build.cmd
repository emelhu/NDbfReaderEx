@ECHO off

set path=%path%;c:\Windows\Microsoft.NET\Framework64\v4.0.30319\

SET configuration=Debug
IF NOT "%1" EQU "" SET configuration=%1

CALL .\nuget-restore.cmd
msbuild .\NDbfReaderEx.sln /t:Rebuild /p:Configuration=%configuration% /v:m /nologo