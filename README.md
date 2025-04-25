# Riesgosutils

Coleccion de clases y funciones recurrentes para el Departamento de Riesgos.

### Instalacion Local

```bash
 pip install -e .
 pip install riesgosutils
```

# DynamoDB

### 1️⃣ Inicialización y conexión a una tabla

```python
db = DynamoDBConnection("../credenciales.json") 
db.connect_table("MiTabla") 

```

### 2️⃣ Insertar un ítem (put_item)

```python
item = {
    "id": "123",
    "nombre": "Ejemplo",
    "edad": 30
}
db.put_item(item)
```

### 3️⃣ Obtener un ítem (get_item)

```python
key = {"id": "123"}
item = db.get_item(key)
print(item)
```

### 4️⃣ Actualizar un ítem (update_item)

```python
key = {"id": "123"}
update_expression = "SET edad = :nueva_edad"
expression_values = {":nueva_edad": 31}

db.update_item(key, update_expression, expression_values)
```

### 5️⃣ Eliminar un ítem (delete_item)

```python
key = {"id": "123"}
db.delete_item(key)
```

### 6️⃣ Escanear la tabla (scan_table)

```python
items = db.scan_table()
print(items)
```

# 7️⃣ Consultar con clave de partición (query_table)

```python
partition_key = "id"
partition_value = "123"

items = db.query_table(partition_key, partition_value)
print(items)
```
