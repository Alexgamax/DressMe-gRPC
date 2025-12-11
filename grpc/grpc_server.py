import grpc
from concurrent import futures
import time

# Importamos los módulos generados
import usuarios_pb2
import usuarios_pb2_grpc
from google.protobuf.timestamp_pb2 import Timestamp

# Conexión a MongoDB
from pymongo import MongoClient
from bson.objectid import ObjectId
from datetime import datetime
import os

# Configuración de MongoDB (db.php)
MONGO_URI = os.getenv('mongo', 'mongodb://db:27017/') # Cambiar 'db:27017' por 'localhost:27017' si vamos a usar Docker si no ps local
client = MongoClient(MONGO_URI)
db = client['DressMeDB']
usuarios_collection = db['usuarios']

# --- Helper para mapear MongoDB a Protobuf ---
def mongo_to_protobuf_usuario(mongo_doc):
    """Convierte un documento de MongoDB a un mensaje Usuario de Protobuf."""
    # Convertir _id de ObjectId a string
    usuario_id = str(mongo_doc.get('_id', ''))
    
    # Manejar la fecha de registro
    fecha_registro = Timestamp()
    if 'fecha_registro' in mongo_doc and isinstance(mongo_doc['fecha_registro'], datetime):
        fecha_registro.FromDatetime(mongo_doc['fecha_registro'])
    
    return usuarios_pb2.Usuario(
        id=usuario_id,
        nombre=mongo_doc.get('nombre', ''),
        email=mongo_doc.get('email', ''),
        password=mongo_doc.get('password', ''),
        genero=mongo_doc.get('genero', ''),
        estilos_preferidos=mongo_doc.get('estilos_preferidos', []),
        # Convertir ObjectIds de listas a string para el mensaje
        prendas_armario=[str(oid) for oid in mongo_doc.get('prendas_armario', [])],
        fecha_registro=fecha_registro,
        seguidores=[str(oid) for oid in mongo_doc.get('seguidores', [])],
        siguiendo=[str(oid) for oid in mongo_doc.get('siguiendo', [])]
    )

def ids_to_objectid_list(ids_list):
    """Convierte una lista de strings de ID a una lista de ObjectId."""
    result = []
    for id_str in ids_list:
        try:
            result.append(ObjectId(id_str))
        except:
            pass 
    return result

# --- Implementación del Servidor gRPC ---
class UsuarioServicer(usuarios_pb2_grpc.UsuarioServiceServicer):
    
    # CREATE (create.php)
    def CrearUsuario(self, request, context):
        try:
            # Documento base
            documento = {
                "nombre": request.nombre,
                "email": request.email,
                "password": request.password,
                "genero": request.genero or '',
                "estilos_preferidos": request.estilos_preferidos,
                "prendas_armario": ids_to_objectid_list(request.prendas_armario),
                "fecha_registro": datetime.utcnow(),
                "seguidores": ids_to_objectid_list(request.seguidores),
                "siguiendo": ids_to_objectid_list(request.siguiendo),
            }
            
            # Insertar en MongoDB
            result = usuarios_collection.insert_one(documento)
            
            return usuarios_pb2.CrudResponse(
                success=True, 
                message="Usuario creado exitosamente", 
                inserted_id=str(result.inserted_id)
            )
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Error al crear usuario: {str(e)}")
            return usuarios_pb2.CrudResponse(success=False, message=str(e))

    # READ (read.php)
    def LeerUsuarios(self, request, context):
        try:
            cursor = usuarios_collection.find({})
            usuarios_list = [mongo_to_protobuf_usuario(doc) for doc in cursor]
            
            return usuarios_pb2.UsuariosList(usuarios=usuarios_list)
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Error al leer usuarios: {str(e)}")
            # Devolver una lista vacía en caso de error
            return usuarios_pb2.UsuariosList(usuarios=[])

    # READ (Uno por ID)
    def LeerUsuarioPorId(self, request, context):
        try:
            object_id = ObjectId(request.id)
            mongo_doc = usuarios_collection.find_one({"_id": object_id})
            
            if mongo_doc:
                return mongo_to_protobuf_usuario(mongo_doc)
            else:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(f"Usuario con ID {request.id} no encontrado")
                # Devolver un Usuario vacío
                return usuarios_pb2.Usuario()
                
        except Exception as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(f"Error al leer usuario: ID inválido o error interno. {str(e)}")
            return usuarios_pb2.Usuario()

    # UPDATE (update.php)
    def ActualizarUsuario(self, request, context):
        try:
            object_id = ObjectId(request.id)
            update_data = {}
            
            # Construir el $set para MongoDB
            if request.nombre: update_data['nombre'] = request.nombre
            if request.email: update_data['email'] = request.email
            if request.password: update_data['password'] = request.password
            if request.genero: update_data['genero'] = request.genero
            if request.estilos_preferidos: update_data['estilos_preferidos'] = request.estilos_preferidos
            if request.prendas_armario: update_data['prendas_armario'] = ids_to_objectid_list(request.prendas_armario)
            if request.seguidores: update_data['seguidores'] = ids_to_objectid_list(request.seguidores)
            if request.siguiendo: update_data['siguiendo'] = ids_to_objectid_list(request.siguiendo)
            
            if not update_data:
                return usuarios_pb2.CrudResponse(success=False, message="No hay campos para actualizar")

            result = usuarios_collection.update_one(
                {"_id": object_id},
                {"$set": update_data}
            )

            if result.modified_count > 0:
                return usuarios_pb2.CrudResponse(success=True, message="Usuario actualizado")
            else:
                return usuarios_pb2.CrudResponse(success=True, message="No se modificó el usuario o no se encontró")

        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Error al actualizar usuario: {str(e)}")
            return usuarios_pb2.CrudResponse(success=False, message=str(e))

    # DELETE (delete.php)
    def EliminarUsuario(self, request, context):
        try:
            object_id = ObjectId(request.id)
            result = usuarios_collection.delete_one({"_id": object_id})
            
            if result.deleted_count > 0:
                return usuarios_pb2.CrudResponse(success=True, message="Usuario eliminado")
            else:
                return usuarios_pb2.CrudResponse(success=False, message="No se encontró el usuario")
                
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Error al eliminar usuario: {str(e)}")
            return usuarios_pb2.CrudResponse(success=False, message=str(e))

# --- Función para iniciar el servidor ---
def serve():
    # Crear un pool de hilos para el servidor
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    # Agregar la implementación del servicio al servidor
    usuarios_pb2_grpc.add_UsuarioServiceServicer_to_server(
        UsuarioServicer(), server)
    
    # Iniciar el servidor en un puerto específico
    server.add_insecure_port('[::]:50051')
    server.start()
    print("Servidor gRPC iniciado en puerto 50051...")
    
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    serve()