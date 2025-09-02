#!/bin/bash

# Setup script for GCP Unused Resources Finder
echo "🔧 Setting up GCP Unused Resources Finder..."

# Check if Python 3 is installed
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is required but not installed. Please install Python 3."
    exit 1
fi

# Install dependencies
echo "📦 Installing Python dependencies..."
pip3 install -r requirements.txt

# Check if gcloud CLI is installed
if ! command -v gcloud &> /dev/null; then
    echo "⚠️  Google Cloud CLI (gcloud) is not installed."
    echo "   Please install it from: https://cloud.google.com/sdk/docs/install"
    echo "   Then run: gcloud auth application-default login"
    exit 1
fi

# Check authentication
echo "🔐 Checking Google Cloud authentication..."
if ! gcloud auth application-default print-access-token &> /dev/null; then
    echo "❌ Google Cloud authentication not found."
    echo "   Please run: gcloud auth application-default login"
    exit 1
fi

echo "✅ Setup completed successfully!"
echo ""
echo "🚀 To run the unused resources scanner:"
echo "   python3 'Unused Resources.py'"
echo ""
echo "📋 The script will:"
echo "   • Find unattached persistent disks"
echo "   • Identify unused static IP addresses"
echo "   • Locate outdated snapshots (>30 days old)"
echo "   • Find unaccessed storage buckets (>90 days inactive)"
echo "   • Generate an Excel report with all findings"
