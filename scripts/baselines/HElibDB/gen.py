import pandas as pd
import numpy as np

t_list = [8+i for i in range(17)]
alpha = ['a', 'b', 'c', 'd', 'e', 'f',
    'g', 'h', 'i', 'j', 'k',
    'l', 'm', 'n', 'o', 'p',
    'q', 'r', 's', 't', 'u',
    'v', 'w', 'x', 'y', 'z']

for o in range(len(t_list)):
    total = pow(2, t_list[o])

    data = []
    number = 0

    for i in range(total):
        temp = alpha[int(i/26/26/26/26/26/26/26)%26] + alpha[int(i/26/26/26/26/26/26)%26] + alpha[int(i/26/26/26/26/26)%26] + alpha[int(i/26/26/26/26)%26] + alpha[int(i/26/26/26)%26] + alpha[int(i/26/26)%26] + alpha[int(i/26)%26] + alpha[i%26]
        temp1 = 'a'*56 + temp
        data.append([temp, temp1])

    data = np.mat(data)
    d = pd.DataFrame(data)
    d.to_csv("kv"+str(t_list[o])+".csv", header=False, index=False)