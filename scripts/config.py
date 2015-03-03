# Python configuration files for pre filtering

# List of 'string's representing files to parse
#
input_files = ['20150223.a']

# List of 'integer's representing hours to keep
# Example : [3,4,5] will keep only hours 3, 4 and 5. Important, it doesn't support ranges
# Empty list means keep all 
#
time = []

# List of 'string's representing msisdn(s) to keep
# Example : ['68f8c9a97f66'] will keep only msisdn 68f8c9a97f66
# Empty list means keep all 
#  
msisdn = ['68f8c9a97f66']
#msisdn = []

# List of 'integer's representing HTTP codes to keep
# Example : [200, 302] will keep only HTTP code 200 and 302
# Empty list means keep all 
#  
retcode = [200, 302]

# List of 'integer's representing the minimum number of bytes to keep
# Example : [10000] will keep only requests with numbytes greater or equal to 10000 bytes
# Empty list means keep all 
#  
numbytes = []

# List of 'string's representing IP address to keep
# Example : ['113.28.240.96'] will keep only '113.28.240.96'
# Empty list means keep all 
#
ip = []

# List of 'string's representing user agents to keep
# Example : [''] will keep only
# Empty list means keep all 
#
useragent = []

# List of 'string's representing path to keep
# Example : [''] will keep only
# Empty list means keep all 
#
path = []

# Verbose mode (print status of each tasks)
verbose = True 

# Send output file on stdout 
output_file_stdout = False 
