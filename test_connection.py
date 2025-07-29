#!/usr/bin/env python3
"""
Simple Rithmic Connection Test
Test basic connection to Rithmic API with current credentials
"""

import asyncio
import logging
import os
from dotenv import load_dotenv
from async_rithmic import RithmicClient

# Load environment variables
load_dotenv()

# Configure detailed logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Enable Rithmic library debug logging
logging.getLogger("rithmic").setLevel(logging.DEBUG)

async def test_connection():
    """Test basic connection to Rithmic"""
    
    # Get credentials from environment
    user = os.getenv('RITHMIC_USER')
    password = os.getenv('RITHMIC_PASSWORD')
    system_name = os.getenv('RITHMIC_SYSTEM_NAME', 'Rithmic Test')
    app_name = os.getenv('RITHMIC_APP_NAME', 'TestApp')
    app_version = os.getenv('RITHMIC_APP_VERSION', '1.0')
    url = os.getenv('RITHMIC_URL', 'rituz00100.rithmic.com:443')
    
    logger.info("=== Rithmic Connection Test ===")
    logger.info(f"User: {user}")
    logger.info(f"System Name: {system_name}")
    logger.info(f"App Name: {app_name}")
    logger.info(f"App Version: {app_version}")
    logger.info(f"URL: {url}")
    logger.info("================================")
    
    # Test different system names
    test_systems = [
        "Rithmic Paper Trading",
        "Rithmic Test", 
        "Rithmic 01",
        "Rithmic Demo"
    ]
    
    logger.info(f"Testing multiple system names: {test_systems}")
    
    if not user or not password:
        logger.error("Missing RITHMIC_USER or RITHMIC_PASSWORD in environment")
        return False
    
    # Test each system name
    for test_system in test_systems:
        logger.info(f"\n🔄 Testing system_name: '{test_system}'")
        try:
            # Create client
            client = RithmicClient(
                user=user,
                password=password,
                system_name=test_system,
                app_name=app_name,
                app_version=app_version,
                url=url
            )
            
            # Add connection event handlers
            async def on_connected(plant_type: str):
                logger.info(f"✅ Successfully connected to plant: {plant_type}")
            
            async def on_disconnected(plant_type: str):
                logger.warning(f"❌ Disconnected from plant: {plant_type}")
            
            client.on_connected += on_connected
            client.on_disconnected += on_disconnected
            
            logger.info(f"Attempting to connect with '{test_system}'...")
            await client.connect()
            
            logger.info(f"✅ SUCCESS! '{test_system}' works! Waiting 3 seconds...")
            await asyncio.sleep(3)
            
            logger.info("Disconnecting...")
            await client.disconnect()
            
            logger.info(f"✅ Test completed successfully with '{test_system}'")
            return True
            
        except Exception as e:
            logger.error(f"❌ '{test_system}' failed: {e}")
            logger.error(f"Error type: {type(e).__name__}")
            continue
    
    logger.error("❌ All system names failed!")
    return False

if __name__ == "__main__":
    success = asyncio.run(test_connection())
    if success:
        print("\n🎉 Connection test PASSED")
    else:
        print("\n💥 Connection test FAILED")
        print("\nPossible solutions:")
        print("1. Check if you have a valid Rithmic demo account")
        print("2. Verify credentials in .env file")
        print("3. Contact Rithmic support for test account setup")
        print("4. Try different SYSTEM_NAME values")