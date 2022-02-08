import numpy as np
import pandas as pd
def reformat_data(x):
  x = str(x).replace('\n','')
  x = x.strip()
  if x == ''or x=='N/A' or x=='nan':
    x = np.NaN
  return x

def covert_numeric(x):
  if not np.isnan(float(x)):
      x = pd.to_numeric(int(float(x)),errors='coerce')
  else:
      x = np.NaN
  return x