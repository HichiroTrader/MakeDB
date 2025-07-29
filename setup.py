#!/usr/bin/env python3
"""
Setup script for RithmicDataCollector
Provides package installation and distribution capabilities
"""

from setuptools import setup, find_packages
import os

# Read the README file
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

# Read requirements
with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

# Get version from environment or default
version = os.getenv("RITHMIC_COLLECTOR_VERSION", "1.0.0")

setup(
    name="rithmic-data-collector",
    version=version,
    author="RithmicDataCollector Team",
    author_email="admin@example.com",
    description="Real-time market data collection system using Rithmic API",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/your-org/rithmic-data-collector",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Financial and Insurance Industry",
        "Intended Audience :: Developers",
        "Topic :: Office/Business :: Financial :: Investment",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
    install_requires=requirements,
    extras_require={
        "dev": [
            "pytest>=6.0",
            "pytest-asyncio>=0.18.0",
            "pytest-cov>=2.0",
            "black>=22.0",
            "flake8>=4.0",
            "mypy>=0.950",
            "pre-commit>=2.0",
        ],
        "monitoring": [
            "psutil>=5.8.0",
            "boto3>=1.20.0",
        ],
        "backup": [
            "boto3>=1.20.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "rithmic-collector=collector:main",
            "rithmic-api=api:main",
            "rithmic-monitor=monitoring:main",
            "rithmic-backup=backup:main",
            "rithmic-test=test_api:main",
        ],
    },
    include_package_data=True,
    package_data={
        "": ["*.json", "*.conf", "*.sql", "*.md", "*.txt"],
    },
    zip_safe=False,
    keywords=[
        "rithmic",
        "market-data",
        "trading",
        "real-time",
        "financial-data",
        "level2",
        "tick-data",
        "futures",
        "api",
        "postgresql",
        "docker",
    ],
    project_urls={
        "Bug Reports": "https://github.com/your-org/rithmic-data-collector/issues",
        "Source": "https://github.com/your-org/rithmic-data-collector",
        "Documentation": "https://github.com/your-org/rithmic-data-collector/wiki",
    },
)