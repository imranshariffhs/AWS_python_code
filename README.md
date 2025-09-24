# 📦 AWS Lambda Layer for Pandas + Numpy

This guide shows how to create a Lambda layer containing **pandas**, **numpy**, and required dependencies, package them correctly, and upload to AWS Lambda.

---

## 🛠️ Prerequisites

- **Python 3.9+** installed (match your Lambda runtime version)
- **pip** installed and available in PATH
- **zip** command-line utility installed

---

## 📂 Steps to Build the Layer

```bash
# 1️⃣ Create a folder structure for the layer
mkdir -p pandas-layer/python

# 2️⃣ Move into the target directory
cd pandas-layer/python

# 3️⃣ Install required libraries into the folder
pip install pandas numpy python-dateutil pytz tzdata -t .

# 4️⃣ Verify installed packages (optional)
ls -lh

# 5️⃣ Move back to root folder
cd ..

# 6️⃣ Create a zip file for the layer
zip -r9 pandas-layer.zip python

# 7️⃣ Verify the zip file
ls -lh
```

---

## 🖇️ Upload to AWS Lambda

1. Go to **AWS Lambda Console**
2. Navigate to **Layers** → **Create Layer**
3. **Name**: `pandas-layer`
4. **Upload** the `pandas-layer.zip`
5. Select your **Python runtime**
6. Click **Create**

---

## ✅ Attach Layer to Lambda Function

1. Open your Lambda function
2. Go to **Configuration → Layers**
3. Click **Add a layer**
4. Choose **Custom layers** → Select `pandas-layer`
5. Choose the version you just uploaded
6. Click **Add**

---

## 🧪 Test the Layer

Inside your Lambda function:

```python
import json
import pandas as pd
import numpy as np

def lambda_handler(event, context):
    df = pd.DataFrame({"numbers": np.arange(5)})
    return {
        "statusCode": 200,
        "body": json.dumps(df.to_dict())
    }
```
