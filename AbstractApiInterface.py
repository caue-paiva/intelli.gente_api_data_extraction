from DataPoint import DataPoint
from abc import ABC , abstractmethod
import pandas as pd

class AbstractApiInterface(ABC):

   DB_CITY_ID_COLUMN = "codigo_municipio" #constantes para o nome das colunas no dataframe final e no banco de dados
   DB_YEAR_COLUMN = "ano"
   DB_DATA_IDENTIFIER_COLUMN = "dado_identificador"
   DB_DATA_VALUE_COLUMN = "valor"
   DB_DTYPE_COLUMN = "tipo_dado"
   MAX_TIME_SERIES_LEN = 7

   api_name:str
   goverment_agency:str

   @abstractmethod
   def __init__(self, api_name:str, goverment_agency:str) -> None:
      pass

   @property #essa função será um campo da classe e seus objetos e poderá ser acessada do jeito:  nome_objeto.DB_TO_API_DATA_MAP
   @abstractmethod 
   def DB_TO_API_DATA_MAP(cls)->dict[str|int, str|int]:
      """
      constante de classe (para cada subclasse da classe abstrata implementar) que mapea o identificador do dado (seu nome ou id, não está decidido)
      que está na base de dados com o identificador desses dados na API por meio de um dicionário.

      Ex da base de agregados do IBGE, mapeando o nome do dado com as variáveis da base de agregados:
      
      {
       "PIB TOTAL" :  37, 
       "PIB AGROPECUARIA": 513   
        ...
      }

      TODO:
      ver se não seria melhor uma função que carrega esse dict de um json ou csv em vez de ter que manualmente escrever um dict grande

      """
      pass

   @abstractmethod   
   def extract_data_points(self, cities:list[int] = [] , data_point_names:list[str] = [] ,  time_series_len: int = 0)->list[DataPoint]:
       pass

   def data_points_to_df(self, data_points: list[DataPoint])->pd.DataFrame:
      """
      Recebe uma lista de pontos de dados (equivalente a uma linha da tabela) e une eles num df no formato dos dados nas tabelas do Data Warehouse
      O algoritmo de transformar a lista em um dataframe da forma mais eficiente foi baseada nessa discussão: https://stackoverflow.com/questions/41888080/efficient-way-to-add-rows-to-dataframe/41888241#41888241

      Args:
         data_points (list[DataPoint]) : lista de pontos de dados para popular as linhas da tabela

      Return:
         (pd.Dataframe) : df no formato da tabela de dados brutos do Data Warehouse
   
      """
      data_point_dict: dict [int, list] = {} #dicionário com a chave sendo o index da linha e o valor sendo uma lista com os valores da linha a ser colocada
      #no df
      
      for index, point in enumerate(data_points): #constroi o dict com os dados da lista de DataPoints
         data_point_dict[index] = [point.city_id, point.year, point.data_name,point.value ,point.data_type.value]

      #coloca as colunas do dataframe
      columns:list[str] = [self.DB_CITY_ID_COLUMN, self.DB_YEAR_COLUMN, self.DB_DATA_IDENTIFIER_COLUMN, self.DB_DATA_VALUE_COLUMN, self.DB_DTYPE_COLUMN]
      df: pd.DataFrame = pd.DataFrame.from_dict(data_point_dict,orient="index",columns=columns) #cria um dataframe a partir do dicionário criado

      return df