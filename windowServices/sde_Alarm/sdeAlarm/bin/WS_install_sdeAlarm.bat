@ECHO OFF
 
REM The following directory is for .NET 2.0
REM set DOTNETFX2=%SystemRoot%\Microsoft.NET\Framework\v4.0.30319
REM set PATH=%PATH%;%DOTNETFX2%
 
echo Uninstalling EventLogger Service...
echo ---------------------------------------------------
InstallUtil /u sdeAlarm.exe
echo ---------------------------------------------------
echo Installing EventLogger Service...
echo ---------------------------------------------------
InstallUtil /i sdeAlarm.exe
echo ---------------------------------------------------
echo Done.
pause >nul