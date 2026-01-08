#!/usr/bin/env python3
"""Test connection to your GPT server on port 1234"""

import requests
import json

print("Testing connection to http://127.0.0.1:1234...")

try:
    # Test models endpoint
    response = requests.get("http://127.0.0.1:1234/v1/models", timeout=5)
    print(f"\n✅ Connected! Status: {response.status_code}")
    
    if response.status_code == 200:
        data = response.json()
        models = data.get('data', [])
        print(f"\nAvailable models ({len(models)}):")
        for model in models:
            print(f"  - {model.get('id')}")
        
        # Test a simple completion
        print("\n\nTesting chat completion...")
        test_payload = {
            "model": models[0]['id'] if models else "gpt-3.5-turbo",
            "messages": [
                {"role": "user", "content": "Say 'OK' if you can hear me"}
            ],
            "temperature": 0
        }
        
        response = requests.post(
            "http://127.0.0.1:1234/v1/chat/completions",
            json=test_payload,
            timeout=30
        )
        
        if response.status_code == 200:
            result = response.json()
            content = result['choices'][0]['message']['content']
            print(f"✅ Model response: {content}")
            print("\n🎉 Everything is working!")
            print("\nYou can now run:")
            print("  python gpt_ranker.py --config ranker_config.toml")
        else:
            print(f"⚠️  Completion failed: {response.status_code}")
            print(response.text)
    
except requests.exceptions.ConnectionError:
    print("❌ Cannot connect to http://127.0.0.1:1234")
    print("\nMake sure your AI server is running!")
    
except Exception as e:
    print(f"❌ Error: {e}")