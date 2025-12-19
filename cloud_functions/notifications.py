import functions_framework
from google.cloud import firestore
from datetime import datetime
import json
import os

# Initialize Firestore
# Note: In Cloud Functions, default credentials are usually sufficient.
try:
    db = firestore.Client()
except Exception as e:
    print(f"Warning: Could not initialize Firestore Client: {e}")
    db = None

def create_notification(notification_data):
    """
    Helper function to write a notification to the 'notifications' collection.
    """
    if not db:
        print("Firestore client not initialized. Cannot send notification.")
        return

    try:
        # Add basic metadata
        notification_data['createdAt'] = firestore.SERVER_TIMESTAMP
        notification_data['read'] = False
        
        # Write to 'notifications' collection
        db.collection('notifications').add(notification_data)
        print(f"Notification created: {notification_data.get('title')}")
        
    except Exception as e:
        print(f"Error creating notification: {e}")

@functions_framework.cloud_event
def on_unidades_proyecto_write(cloud_event):
    """
    Triggered when a document is written to 'unidades_proyecto' collection.
    """
    data = cloud_event.data
    
    event_type = cloud_event["type"] # e.g., google.cloud.firestore.document.v1.written
    resource = cloud_event["source"]
    
    print(f"Event type: {event_type}")
    print(f"Resource: {resource}")

    # Parse Firestore Event Data
    # The 'data' payload in CloudEvent for Firestore is complex (contains 'oldValue' and 'value')
    # Use helpful libraries or manual parsing if needed. 
    # For now, we will extract basic info.
    
    value = data.get("value")
    old_value = data.get("oldValue")
    
    if not value: 
        # Document was deleted
        return

    new_fields = value.get("fields", {})
    old_fields = old_value.get("fields", {}) if old_value else {}
    
    # Example logic: Detect if it's a new document or an update
    is_new = not old_value
    
    upid = new_fields.get("upid", {}).get("stringValue", "Unknown")
    nombre = new_fields.get("nombre_proyecto", {}).get("stringValue", "Sin nombre")

    if is_new:
        notification = {
            "type": "NEW_PROJECT_UNIT",
            "title": "Nueva Unidad de Proyecto",
            "message": f"Se ha creado la unidad: {nombre} ({upid})",
            "data": {
                "upid": upid,
                "collection": "unidades_proyecto",
                "docId": resource.split("/")[-1]
            },
            "destinedUser": "all" # Broadcast behavior
        }
        create_notification(notification)
    else:
        # Check for specific changes, e.g., state change
        # This assumes a 'estado' field exists
        new_state = new_fields.get("estado", {}).get("stringValue")
        old_state = old_fields.get("estado", {}).get("stringValue")
        
        if new_state and new_state != old_state:
            notification = {
                "type": "PROJECT_UNIT_UPDATE",
                "title": "Actualización de Unidad",
                "message": f"La unidad {nombre} cambió de estado a: {new_state}",
                "data": {
                    "upid": upid,
                    "collection": "unidades_proyecto",
                    "docId": resource.split("/")[-1],
                    "oldState": old_state,
                    "newState": new_state
                },
                "destinedUser": "all"
            }
            create_notification(notification)

@functions_framework.cloud_event
def on_contrato_write(cloud_event):
    """
    Triggered when a document is written to 'contratos' collection.
    """
    data = cloud_event.data
    value = data.get("value")
    old_value = data.get("oldValue")
    
    if not value:
        return # Deleted

    new_fields = value.get("fields", {})
    is_new = not old_value
    
    contrato_id = new_fields.get("numero_contrato", {}).get("stringValue", "S/N")
    objeto = new_fields.get("objeto", {}).get("stringValue", "Sin objeto")

    if is_new:
        notification = {
            "type": "NEW_CONTRACT",
            "title": "Nuevo Contrato",
            "message": f"Nuevo contrato registrado: {contrato_id}",
            "data": {
                "contractId": contrato_id,
                "collection": "contratos"
            },
            "destinedUser": "all"
        }
        create_notification(notification)
