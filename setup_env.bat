

call "D:\Program Files\Microsoft Visual Studio\2022\Enterprise\VC\Auxiliary\Build\vcvars64.bat"

set LIBCLANG_PATH=C:\Program Files\LLVM\bin

set CC=cl
set CXX=cl


echo LIBCLANG_PATH=%LIBCLANG_PATH%
echo CC=%CC%
echo CXX=%CXX%
echo VCINSTALLDIR=%VCINSTALLDIR%
echo WindowsSdkDir=%WindowsSdkDir%
echo WindowsSDKVersion=%WindowsSDKVersion%
echo INCLUDE=%INCLUDE%

cargo build --bin ord --release
