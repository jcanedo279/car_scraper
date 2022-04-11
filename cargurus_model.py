import torch
import math
import numpy as np

from collections import OrderedDict

torch.manual_seed(50)


class Add_k(torch.nn.Module):
    def __init__(self, k):
        super(Add_k, self).__init__()
        self.k = k
    def forward(self, x):
        return x + self.k


class CarGurus_Model(torch.nn.Module):
    
    def __init__(self, size_input):
        
        super(CarGurus_Model, self).__init__()
        
        ACT_FN = torch.nn.ReLU()
        # ACT_FN = torch.nn.SELU()
        # ACT_FN = torch.nn.Sigmoid()
        
        if size_input == 1:
            layers = [(f'fc0', torch.nn.Linear(1, 1))]
        else:
            layer_ind, cur_inp_size = 1, size_input
            # layers = [(f'fc0', torch.nn.Linear(size_input, size_input)), (f'act0', ACT_FN)]
            layers = []
            while cur_inp_size > 1:
                cur_out_size = max(math.floor(cur_inp_size/2), 1)
                
                layers.append((f'fc{layer_ind}', torch.nn.Linear(cur_inp_size, cur_out_size)))
                layers.append((f'act{layer_ind}', ACT_FN))
                
                layer_ind += 1
                cur_inp_size = cur_out_size
            layers.pop()
        
        
        self.inner_model = torch.nn.Sequential(OrderedDict( layers ))
    
    def forward(self, x):
        y_pred = self.inner_model(x)
        # y_pred = self.network2(x)
        return y_pred
        
