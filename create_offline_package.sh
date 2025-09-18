#!/bin/bash
# MongoDB Log Analyzer - Offline Package Creator
# This script creates an offline package bundle for deployment without internet access

echo "🚀 Creating offline package bundle for MongoDB Log Analyzer..."

# Create directories
mkdir -p offline_deployment/packages
mkdir -p offline_deployment/scripts

# Download all required packages
echo "📦 Downloading Python packages..."
pip download -r requirements.txt -d offline_deployment/packages/ --prefer-binary

echo "📝 Creating installation script..."

# Create offline installation script
cat > offline_deployment/install_offline.sh << 'EOF'
#!/bin/bash
# MongoDB Log Analyzer - Offline Installation Script

echo "🔧 Installing MongoDB Log Analyzer offline packages..."

# Check if pip is available
if ! command -v pip &> /dev/null; then
    echo "❌ pip is not installed. Please install Python and pip first."
    exit 1
fi

# Install packages from offline directory
echo "📦 Installing packages..."
pip install --find-links packages/ --no-index -r requirements.txt

if [ $? -eq 0 ]; then
    echo "✅ All packages installed successfully!"
    echo "🚀 You can now run the MongoDB Log Analyzer with: python app.py"
else
    echo "❌ Installation failed. Please check the error messages above."
    exit 1
fi
EOF

# Copy requirements.txt to offline deployment
cp requirements.txt offline_deployment/

# Create README for offline deployment
cat > offline_deployment/README_OFFLINE.txt << 'EOF'
MongoDB Log Analyzer - Offline Deployment Package
================================================

This package contains all the necessary Python packages to run the MongoDB Log Analyzer
without an internet connection.

INSTALLATION STEPS:
------------------

1. Ensure Python 3.7+ is installed on the target system
2. Copy this entire 'offline_deployment' folder to the target system
3. Navigate to the offline_deployment folder
4. Run the installation script:

   Linux/Mac:
   chmod +x install_offline.sh
   ./install_offline.sh

   Windows:
   python -m pip install --find-links packages/ --no-index -r requirements.txt

5. Copy the main MongoDB Log Analyzer files to your desired location
6. Run the application: python app.py

PACKAGE CONTENTS:
----------------
- packages/: All Python packages (.whl and .tar.gz files)
- requirements.txt: Package dependencies list
- install_offline.sh: Automated installation script
- README_OFFLINE.txt: This documentation

MANUAL INSTALLATION:
-------------------
If the automated script doesn't work, you can install manually:

pip install --find-links packages/ --no-index -r requirements.txt

VERIFICATION:
------------
After installation, you can verify the packages are installed:

pip list

The following packages should be present:
- Flask (2.3.3)
- Werkzeug (2.3.7)
- Jinja2 (3.1.2)
- pandas (2.0.3)
- python-dateutil (2.8.2)
- requests (2.31.0)

TROUBLESHOOTING:
---------------
- If installation fails, ensure you have sufficient permissions
- For Windows, you may need to run the command prompt as Administrator
- If packages conflict, create a virtual environment first:
  python -m venv myenv
  source myenv/bin/activate  (Linux/Mac)
  myenv\Scripts\activate     (Windows)

Then run the installation command.
EOF

# Make installation script executable
chmod +x offline_deployment/install_offline.sh

echo "✅ Offline package bundle created successfully!"
echo ""
echo "📁 Package contents:"
echo "   - offline_deployment/packages/ - All required Python packages"
echo "   - offline_deployment/install_offline.sh - Automated installation script"
echo "   - offline_deployment/requirements.txt - Package list"
echo "   - offline_deployment/README_OFFLINE.txt - Documentation"
echo ""
echo "📋 To deploy offline:"
echo "   1. Copy the 'offline_deployment' folder to target system"
echo "   2. Run: cd offline_deployment && ./install_offline.sh"
echo "   3. Copy your MongoDB Log Analyzer files"
echo "   4. Run: python app.py"
echo ""
echo "🎯 Total package size:"
du -sh offline_deployment/ 2>/dev/null || echo "   [Size calculation available after running]"