cd code

echo "Building Returns"
(cd 01_build-returns ; bash build-returns.sh)

echo "Building MEDSL"
(cd 02_build-medsl ; bash build-medsl.sh)

# echo "Building Harvard"
# (cd 03_build-harvard ; bash build-harvard.sh)

echo "Building Release"
(cd 04_build-release ; bash build-release.sh)

echo "Building Paper"
(cd 05_paper-analyze ; bash analysis.sh)
