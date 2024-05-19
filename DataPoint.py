from enum import Enum
from typing import Any

class DataPointTypes(Enum):
   INT = "int"
   FLOAT = "float"
   STRING = "str"
   BOOL = "bool"


class DataPoint:
   """
   Essa classe tem uma relação quase 1 <-> 1 com um linha de uma tabela do BD de categorias com dados brutos (a única diferença seria o campo de multiply amount)
   mas ele é usado para calcular o campo value e não será colocado na tabela
   
   """
   city_id: int
   year: int
   data_name: str
   data_type: DataPointTypes
   value: Any
   multiply_amount:int|float


   def __init__(
      self, 
      city_id: int, 
      year: int, 
      data_name: str, 
      data_type: DataPointTypes,  
      multiply_amount: int|float = None,
      value: Any = None
   ) -> None:
      
      self.city_id = city_id
      self.year = year
      self.data_name = data_name
      self.data_type = data_type
      self.value = value

      if multiply_amount is None:
         self.multiply_amount = 1
      else:
         if self.data_type not in [DataPointTypes.INT, DataPointTypes.FLOAT]:
            raise IOError("Valor de multiplicação não é valido para tipos que não sejam inteiros ou float")
         self.multiply_amount = multiply_amount

   def infer_dtype_and_multiply_amnt(self, unit_description_str:str)->bool:
      """
      APIs como a do ibge tem um campo chamado "unidade", onde é explicado qual unidade o dado se refere, essa unidade pode seguir um padrão como
      "mil reais" e a partir desses padrões é possível inferir o tipo de dado e quanto precisa multiplicar o dado
      
      
      """
      sucess_flag:bool = True #vai ser retornado true se foi possível inferir tanto o tipo de dado quanto a qntd pra multiplicar da string

      lowercase_str: str = unit_description_str.lower()
      multiply_amnt_map: dict[str,int] = {
         "mil" : 1000,
         "cem" : 100
      }

      for key in multiply_amnt_map:
         if key in lowercase_str:
            self.multiply_amount = multiply_amnt_map[key]
            break
      else:
         self.multiply_amount = 1
         sucess_flag = False


      dtype_map: dict[str,DataPointTypes] = {
         "reais" : DataPointTypes.FLOAT,
         "real"  : DataPointTypes.FLOAT
      }

      for key in dtype_map:
         if key in lowercase_str:
            self.data_type = dtype_map[key]
            break
      else:
         sucess_flag = False

      return sucess_flag
   
   def multiply_value(self, value:Any)-> int | float:
      if self.data_type not in [DataPointTypes.INT, DataPointTypes.FLOAT]:
         return value
      else:
         if self.data_type == DataPointTypes.INT:
            converted_val = int(value) 
         elif self.data_type == DataPointTypes.FLOAT:
            converted_val = float(value)
         
         return converted_val * self.multiply_amount