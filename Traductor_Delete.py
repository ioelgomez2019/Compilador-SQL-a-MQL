"""
Created on Tue Feb 16 12:55:55 2021

@author: JOEL
"""

import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Where

# BLOCCO MAIN
stringa1 = 'DELETE FROM molise;'
stringa111 = 'DELETE FROM estudiates'
stringa2 = 'DELETE FROM people WHERE status = "D"'
#stringa3 = 'DELETE FROM people WHERE status = "D" and name <= "Carlo" or name != "Saretta"'
#stringa4 = 'DELETE FROM people WHERE status = "D" or name <= "Carlo" and name != "Saretta"'
#stringa5 = 'DELETE FROM people WHERE status = "D" and name <= "Carlo" and name != "Saretta"'
#stringa6 = 'DELETE FROM people WHERE status = "D" or name <= "Carlo" and name != "Saretta" or age >= 18'


# DELETE WHERE - elimina le tuple della tabella con una certa condizione
def delete(tokens):
  table = ""
  where_found = False
# scorre tutti i token per individuare il nome della tabella e trovare la condizione del where,
# che viene memorizzata nel vettore parsed.
# (es) parsed -> ['status', '=', '"D"', 'OR', 'name', '<=', '"Carlo"', 'AND', 'name', '!=', '"Saretta"']
  for token in tokens:
    if isinstance(token, Identifier):
      table = token.value
    if isinstance(token, Where):
      where_found = True
      output = convert_where_condition(token)
      # Se nella condizione del where erano presenti degli operatori logici 
      # allora utilizzali per la costruzione della query finale, altrimenti
      # se era una condizione semplice costruisci la query finale con solamente
      # l'unico selettore presente.
      if isinstance(output[0],LogicOperator):
        final_query = "db."+ table +".deleteMany(" + output[-1].created_string + ")"
      else:
        output_parenthesis = convert_condition_in_mongo(output) 
        final_query = "db." + table + ".deleteMany(" + str(output_parenthesis[0]) + ")"
  if not where_found:
    final_query = "db."+str(table)+".deleteMany({})"
  return(final_query)



# Una volta trovata la condizione del where, scorre tutti i suoi token 
# per memorizzare nel dizionario logic_op_positions il tipo di operatore logico 
# e la sua posizione all'interno della condizione where.
# (es) logic_op_positions -> {3: 'OR', 7: 'AND'}

def create_op_positions(parsed):
  logic_op_positions = {}
  for i, item in enumerate(parsed, start = 0):
    if item == "AND" or item == "OR":
      logic_op_positions[i] = "{0}".format(item)
  return logic_op_positions

# Crea il vettore 2D where_list_2D, che contiene al suo interno tante liste
# di token quante sono le sottocondizioni di cui è composto il where.
# (es) where_list_2D -> [['status', '=', '"D"'], ['name', '<=', '"Carlo"'], ['name', '!=', '"Saretta"']]

def create_subconditions_list(logic_op_positions, parsed):
  starting_pos = 0
  where_list_2D = []
  for key, value in logic_op_positions.items():
    temp_list = parsed[starting_pos:key]
    where_list_2D.append(temp_list)
    starting_pos = key + 1
    # Nel momento in cui viene raggiunto l'ultimo operatore logico, 
    # deve essere costruita l'ultima sottolista di where_list_2D prendendo 
    # tutti i token rimamente in parsed.
    if key == list(logic_op_positions.items())[-1][0]: 
      temp_list = parsed[starting_pos:len(parsed)]
      where_list_2D.append(temp_list)
  return where_list_2D

# A questo punto avviene la traduzione delle sottocondizione da sql a sintassi mongodb,
# inanzitutto traducendo il selettore (che è sempre l'elemento centrale di ogni sottolista)
# e poi memorizzando il risultato finale nel vettore output_parenthesis.
# (es) output_parenthesis -> ['{ status: { $eq: "D" }}', '{ name: { $lte: "Carlo" }}', '{ name: { $ne: "Saretta" }}']
def convert_subconditions_in_mongo(where_list_2D):
  output_parenthesis = []
  for item in where_list_2D:
    if item[1] == "=":
      selector = "$eq"
    elif item[1] == "!=":
      selector = "$ne"
    elif item[1] == ">":
      selector = "$gt"
    elif item[1] == ">=":
      selector = "$gte"
    elif item[1] == "<":
      selector = "$lt"
    elif item[1] == "<=":
      selector = "$lte"
    output_inner = "{ "+ item[0] +": { "+selector+": " + item[2] + " }"
    output_parenthesis.append(output_inner + "}")
  return output_parenthesis

# Viene creato il vettore logic_op_priorities assegnando ad ogni operatore logico
# una differente priorità: dovranno infatti essere prima eseguiti tutti gli AND
# da sinistra a destra, e poi tutti gli OR da sinistra a destra.
# (es) logic_op_priorities -> [7, 3]
def create_logic_ops(logic_op_positions):
  logic_op_priorities = []
  logic_ops = []
  for key, value in logic_op_positions.items():
    if value == "AND":
      logic_op_priorities.append(key)
  for key, value in logic_op_positions.items():
    if value == "OR":
      logic_op_priorities.append(key)

  # Vengono ora unite le posizioni, le priorità e i valori (AND/OR) 
  # di tutti gli operatori logici negli oggetti LogicOperator,
  # i quali vengono aggiunti alla lista logic_ops.
  # (es) logic_ops[0] -> {'pos': 3, 'ttype': 'OR', 'priority': 1, 'left': None, 'right': None, 'created_string': None}
  for key, value in logic_op_positions.items():
    op = LogicOperator()
    op.pos = key
    op.ttype = value
    op.priority = logic_op_priorities.index(key)
    logic_ops.append(op)
  return logic_ops

# Per ogni sottolista contenuta in where_list_2D viene creato un oggetto Block,
# e aggiunto alla lista blocks. Ogni blocco avrà come attributi l'id/posizione del blocco,
# il valore in sql (estratto da where_list_2D)
# ed il valore in mongodb (estratto da output_parenthesis).
# (es) blocks[0] -> {'id': 0, 'sql_value': ['status', '=', '"D"'], 'mongo_value': '{ status: { $eq: "D" }}', 'mapped_by': None}

def create_blocks(where_list_2D, output_parenthesis):
  blocks = []
  for i, item in enumerate(where_list_2D, start = 0):
    block = Block(i, item, output_parenthesis[i])
    blocks.append(block) 
  return blocks

# Ogni operatore logico in logic_ops viene mappato con la sottocondizione di sinistra (op.left)
# e con la sottocondizione di destra (op.right) in base alla sua posizione
# relativa nella condizione where.
# Le sottocondizioni possono essere dei blocchi (nel caso degli operatori con priorità
# di esecuzione maggiore) oppure il risultato di altri operatori eseguiti precedentemente.
# (es) logic_ops[0] -> {'pos': 7, 'ttype': 'AND', 'priority': 0, 'left': <__main__.Block object at 0x7f12d8b5ee80>, 'right': <__main__.Block object at 0x7f12d8b5ee48>, 'created_string': None}
def mapping(logic_ops, blocks):
  for op in logic_ops:
    rel_block_pos = op.pos//3
    left_block_id = rel_block_pos - 1
    right_block_id = rel_block_pos
    for block in blocks:
      if block.id == left_block_id and block.mapped_by == None:
        block.mapped_by = op
        op.left = block
      elif block.id == left_block_id and block.mapped_by != None:
        op.left = block.mapped_by
      elif block.id == right_block_id and block.mapped_by == None:
        block.mapped_by = op
        op.right = block
      elif block.id == right_block_id and block.mapped_by != None:
        op.right = block.mapped_by

# Viene eseguita la traduzione in mongoDB degli operatori, 
# partendo da quelli con priorità maggiore e memorizzando il risultato
# parziale nell'attributo created_string del blocco. Gli operatori successivi
# che hanno come mapping left o right quel blocco appena eseguito costruiranno
# in modo incrementale il risultato a partire dal valore di created_string.
# Il risultato finale della traduzione sarà quindi contenuto nell'attributo
# created_string dell'ultimo blocco.
def execute_ops (logic_ops):
  id_last_executed_op = None
  for op in logic_ops:
    if isinstance(op.left, Block) and isinstance(op.right, Block):
      left_value = op.left.mongo_value
      right_value = op.right.mongo_value
      id_last_executed_op = op.pos
    elif isinstance(op.left, LogicOperator) and isinstance(op.right, Block):
      left_value = find(logic_ops,id_last_executed_op).created_string
      right_value = op.right.mongo_value
      id_last_executed_op = op.pos
    elif isinstance(op.left, Block) and isinstance(op.right, LogicOperator):
      left_value = op.left.mongo_value
      right_value = find(logic_ops,id_last_executed_op).created_string
      id_last_executed_op = op.pos
    elif isinstance(op.left, LogicOperator) and isinstance(op.right, LogicOperator):
      left_value = find(logic_ops,id_last_executed_op).created_string
      right_value = find(logic_ops,id_last_executed_op).created_string
      id_last_executed_op = op.pos
    op.created_string  = "{$" + op.ttype.lower() + ": [" + str(left_value) + ", " +  str(right_value) + "]}"

def convert_condition_in_mongo(parsed):
  output_parenthesis = []
  selector = ""
  for item in parsed:
    if item == "=":
      selector = "$eq"
    elif item == "!=":
      selector = "$ne"
    elif item == ">":
      selector = "$gt"
    elif item == ">=":
      selector = "$gte"
    elif item == "<":
      selector = "$lt"
    elif item == "<=":
      selector = "$lte"
  output_inner = "{ "+ parsed[0] +": { "+selector+": " + parsed[2] + " }"
  output_parenthesis.append(output_inner + "}")
  return output_parenthesis

def convert_where_condition(token):
  parsed = token.value.split(" ")
  parsed.remove("WHERE")
  logic_op_positions = create_op_positions(parsed)
  if logic_op_positions:
    where_list_2D = create_subconditions_list(logic_op_positions, parsed)
    output_parenthesis = convert_subconditions_in_mongo(where_list_2D)
    logic_ops = create_logic_ops(logic_op_positions)
    blocks = create_blocks(where_list_2D, output_parenthesis)
   # Gli operatori logici vengono ordinati in base alla loro priorità di esecuzione.
    logic_ops.sort(key=lambda x: x.priority, reverse=False)
    mapping(logic_ops,blocks)
    execute_ops(logic_ops)
    output = logic_ops
  else:
    output = parsed
  return output

# Classe dell'operatore logico AND/OR. La pos indica la posizione all'interno della condizione where,
# (e quindi funge da id), il ttype indica il tipo AND/OR, la priorità l'ordine di esecuzione, left e right
# le sottocondizione di sinistra e di destra, mentre su created_string viene memorizzato il risultato 
# della sua traduzione in sintassi mongoDB.
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


# La classe blocco indica una sottocondizione che precede o segue un operatore logico 
# nella condizione del where iniziale. Esso è quindi caratterizzato da un id (posizione del blocco), 
# da una traduzione in sql (sql_value) ed uno in mognodb (mongo_value).
# L'attributo mapped_by serve per mappare un operatore successivo con uno precedente 
# che ha già mappato quel blocco.
class Block:
  def __init__(self, id, sql_value = None, mongo_value = None, mapped_by = None):
    self.id = id
    self.sql_value = sql_value
    self.mongo_value = mongo_value
    self.mapped_by = mapped_by
  def __str__(self):
    return (str(self.__class__) + ": " + str(self.__dict__))

# Funzione per trovare un operatore logico in logic_ops tramite il suo id/pos
def find(list, id):
  result = None
  for item in list:
    if item.pos == id:
      result = item
  return result
