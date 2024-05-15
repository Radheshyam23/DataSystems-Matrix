import pandas as pd
import numpy as np

# Create a csv file with column names A to Z and 10000 rows
df = pd.DataFrame(columns=[chr(i) for i in range(65, 91)], index=range(1000))

# Fill the dataframe with random values
df = df.applymap(lambda x: np.random.randint(0, 100))

# Save the dataframe to a csv file
df.to_csv('MidTable.csv', index=False)