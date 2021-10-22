# from: https://docs.aws.amazon.com/lambda/latest/dg/python-package.html

pip install --target ./package -r requirements.txt
cd package
zip -r ../my-deployment-package.zip .
cd ..
zip -g my-deployment-package.zip lambda_function.py
