#!/bin/bash
set -e

echo "Esperando a que MongoDB esté listo..."
sleep 5

DB_NAME="DressMeDB"

echo "Verificando si la base de datos existe..."
DB_EXISTS=$(mongosh --quiet --eval "db.getMongo().getDBNames().includes('$DB_NAME')")
echo "Resultado: $DB_EXISTS"

if [ "$DB_EXISTS" = "false" ]; then
    echo "La base de datos no existe. Creándola..."
    mongosh --eval "use $DB_NAME"
else
    echo "La base de datos ya existe."
fi

echo "Importando colecciones..."

for file in /docker-entrypoint-initdb.d/*.json
do
    COLLECTION=$(basename "$file" .json)

    echo "Importando colección '$COLLECTION' desde $file"

    mongoimport \
        --db "$DB_NAME" \
        --collection "$COLLECTION" \
        --file "$file" \
        --jsonArray \
        --mode=upsert \
        --maintainInsertionOrder

done

echo "Inicialización completada."
