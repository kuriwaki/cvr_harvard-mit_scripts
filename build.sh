
echo "Building MEDSL"
cd code/build-medsl
bash build-medsl.sh
cd ..
cd ..

echo "Building Returns"
cd code/build-returns
bash build-returns.sh
cd ..
cd ..

echo "Building Release"
cd code/build-release
bash build-release.sh
cd ..
cd ..
