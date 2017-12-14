# Script for testing single-package builds
# To be removed once scons sphinx is available

sphinx-build -b html -d _build/doctrees . _build/html
echo The documentation is available at _build/html/index.html
