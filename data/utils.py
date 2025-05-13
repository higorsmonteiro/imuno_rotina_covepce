# -*- coding: utf-8 -*- 
'''
    Função de utilidades gerais.

    Autor: Higor S. Monteiro
    Email: higormonteiros@gmail.com
'''

import pandas as pd
from unidecode import unidecode
import tkinter as tk
from tkinter import filedialog
from sqlalchemy import text

def select_folder(msg="Select a Folder"):
    root = tk.Tk()
    root.withdraw()  # Hide the root window
    folder_path = filedialog.askdirectory(
        initialdir="/",  # Set the initial directory
        title=msg,  # Set the dialog title
        mustexist=True  # Only allow selection of existing folders
    )
    return folder_path

def uniformize_name(string, sep=''):
    '''
        Modify the input string to a final string without any special character and numbers.

        Args:
        -----
            string:
                String. Input name to uniformize.
            sep:
                String. Separator for different chunks of the string.
        Return:
        -------
            new_string:
                String. Final string.
    '''
    if sep=='':
        new_string = sep.join([char.upper() for char in string if char.isalnum()])
        new_string = sep.join([char.upper() for char in new_string if not char.isdigit()])
    elif sep==' ':
        new_string = []
        string_lst = string.split(sep)
        for s in string_lst:
            new_string.append(''.join([char.upper() for char in s if char.isalnum()]))
        new_string = sep.join(new_string)
    new_string = unidecode(new_string)
    return new_string


def cns_is_valid(cns):
    """
    Função para validar número do CNS - Cartão Nacional de Saúde ou Cartão do SUS.

    Args:
    -----
        cns:
            String.
    
    Return:
    -------
        Boolean.
    """
    cns = ''.join(filter(str.isdigit, str(cns)))
    
    if len(cns) != 15:
        return False
    if cns=="000000000000000" or pd.isna(cns):
        return False
    
    return sum(
        [int(cns[i]) * (15 - i) for i in range(15)]
    ) % 11 == 0


