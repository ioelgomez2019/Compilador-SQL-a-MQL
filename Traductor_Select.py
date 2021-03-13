# -*- coding: utf-8 -*-
"""
Created on Tue Feb 16 12:55:55 2021

@author: JOEL
"""

import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Where, Function

#consultas que acepta
stringa1 = 'SELECT * FROM people'         #//////////////////////////
#stringa2 = 'SELECT id, user_id, status FROM people order by status asc limit 3'  #//////////////////////////
stringa3 = 'SELECT user_id FROM people'  #//////////////////////////

def select(tokens):
  table = "" #aqui entran en toda la cadena 
  where_found = False
  select_found = False
  limit_found = False
  wildcard_found = False
  wildcard_count_found = False
  distinct_found = False
  count_found = False
  output_parenthesis_select_attributes = ""
  order_by_found = False
  order_by_rule = ""
  count_field = ""
  count_field_found = False

# itera a través de todos los tokens para ubicar el nombre de la tabla y encontrar la condición where,
# que se almacena en el vector analizado.
# (es) parsed -> ['status', '=', '"D"', 'OR', 'name', '<=', '"Carlo"', 'AND', 'name', '!=', '"Saretta"']
  for token in tokens:
    if token.value == "SELECT":
      select_found = True
    if token.value == "*":
      wildcard_found = True
      select_found = False




    if isinstance(token, Identifier) and not select_found and not order_by_found:
      table = token.value
    if isinstance(token,Identifier) and select_found :
      select_found = False
      output_parenthesis_select_attributes = convert_single_select_attribute(token) 
    if isinstance(token,IdentifierList) and select_found:
      select_found = False
      output_parenthesis_select_attributes = convert_multiple_select_attributes(token)



    if isinstance(token, Where):
      where_found = True
      output = convert_where_condition(token)
      # Si los operadores lógicos estuvieran presentes en la condición where
      # luego utilícelos para la construcción de la consulta final, de lo contrario
      # si se trataba de una condición simple, cree la consulta final con solo
      # el único selector presente.
      comma = ""

      if isinstance(output[0],LogicOperator):
        if distinct_found:
          output_parenthesis_distinct = '.distinct("' + distinct_value + '", ' + output[-1].created_string + ")"
          final_query = "db." + table + output_parenthesis_distinct
        if output_parenthesis_select_attributes != "" and not distinct_found:
          comma = ","
        if limit_found:
          limit_found = False
          final_query = "db."+ table +".find(" + output[-1].created_string + comma + " " + output_parenthesis_select_attributes + ")" + output_parenthesis_limit
        elif count_field_found:
          final_query = "db."+ table +".find(" + output[-1].created_string + comma + " " + output_parenthesis_select_attributes + "{" + count_field + ": {exists:true}}" + ")"
        else:
          final_query = "db."+ table +".find(" + output[-1].created_string + comma + " " + output_parenthesis_select_attributes + ")"

      elif output[1] == "like":
        original = output[2].replace('"', '')
        positions = [pos for pos, char in enumerate(original) if char == "%"]
        like_arg = original.replace("%", "/")
        if len(positions) == 1:
          if positions[0] == 0:
             like_arg = like_arg[:len(original)] + "$/" + like_arg[len(original):]
          elif positions[0] == len(original) - 1 :
            like_arg = like_arg[:0] + "/^" + like_arg[0:]
        final_query = "db." + table + ".find({" + output[0] + ": " + like_arg + "})"



  if not where_found:
    if distinct_found:
      final_query = "db."+ str(table) + output_parenthesis_distinct
    else:
      if wildcard_found:
        final_query = "db."+str(table)+".find({})"
      elif count_field_found:
        final_query = "db."+ table +".find({" + count_field + ": {exists:true}}" + ")" 
      elif output_parenthesis_select_attributes != "" :
        final_query = "db."+str(table)+".find({}," + output_parenthesis_select_attributes + ")"
      else:
        final_query = "db."+str(table)+".find({})"
  if order_by_found:
    final_query = final_query + ".sort({ " + order_by_column + ": " + order_by_rule + "})"
  if limit_found:
    final_query = final_query + output_parenthesis_limit
  if count_found:
    final_query = final_query + ".count()"

  return(final_query)






# Los operadores se traducen a mongoDB,
# comenzando por aquellos con mayor prioridad y almacenando el resultado
# parcial en el atributo created_string del bloque. Los operadores posteriores
# que tienen un mapeo izquierdo o derecho que el bloque recién ejecutado se construirá
# incrementalmente el resultado a partir del valor de created_string.
# El resultado final de la traducción estará contenido en el atributo


# Clase del operador lógico Y / O. La pos indica la posición dentro de la condición where,
# (y por lo tanto actúa como id), el ttype indica el tipo Y / O, la prioridad el orden de ejecución, izquierda y derecha
# las subcondiciones izquierda y derecha, mientras que el resultado se almacena en created_string
# de su traducción a la sintaxis mongoDB.
class LogicOperator:
    def __init__(self, pos = None, ttype = None, priority = None, left = None, right = None, created_string = None):
      self.pos = pos
      self.ttype = ttype
      self.priority = priority
      self.left = left
      self.right = right
      self.created_string = created_string
    def __str__(self):
      return (str(self.__class__) + ": " + str(self.__dict__))


# La clase de bloque indica una subcondición que precede o sigue a un operador lógico
# en la condición inicial where. Por lo tanto, se caracteriza por un id (posición del bloque),
# da una traducción en sql (sql_value) y otra en mognodb (mongo_value).
# El atributo mapped_by se utiliza para mapear un operador posterior a uno anterior.
# que ya ha asignado ese bloque.
class Block:
  def __init__(self, id, sql_value = None, mongo_value = None, mapped_by = None):
    self.id = id
    self.sql_value = sql_value
    self.mongo_value = mongo_value
    self.mapped_by = mapped_by
  def __str__(self):
    return (str(self.__class__) + ": " + str(self.__dict__))

# Función para encontrar un operador lógico en logic_ops por su id / pos
def find(list, id):
  result = None
  for item in list:
    if item.pos == id:
      result = item
  return result

def convert_single_select_attribute(token):
  if (token.value == "id"):
    output_parenthesis_select = "{_id: 1}"
  else:
    output_parenthesis_select = "{ " + token.value + ": 1, " + "_id: 0}"
  return output_parenthesis_select

def convert_multiple_select_attributes(token):
  elements = []
  id_found = False
  output_parenthesis_select = ""
  for elem in token:
    if isinstance(elem, Identifier):
      if (elem.value == "id"):
        id_found = True
      else:
        output = elem.value + ": 1"
        elements.append(output)
  
  output_string_noquotes = output_string.replace("'", "")
 
  if not id_found:
    output_parenthesis_select = "{ " + output_string_noquotes +  ", _id: 0 }"
  else: 
    output_parenthesis_select = "{ " + output_string_noquotes +  " }"

  return output_parenthesis_select
