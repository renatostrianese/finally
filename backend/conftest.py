# backend/conftest.py
# pytest configuration for the backend package
import sys
import os

# Make the backend/ directory itself available as the root for imports
# so tests can do `from market.xxx import ...`
sys.path.insert(0, os.path.dirname(__file__))
