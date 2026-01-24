# Install dependency
pip install XenAPI

# First run - create config file
python xcp_admin.py --create-config

# Run with logging
python xcp_admin.py --host xcpng03 --log

# Or just run and it will prompt
python xcp_admin.py
