"""VRP con múltiples vehículos, capacidad, ventanas de tiempo y pickup y delivery."""

from ortools.constraint_solver import routing_enums_pb2
from ortools.constraint_solver import pywrapcp
import pandas as pd
from access_db import ConfiguracionConexion, AccessDB
import folium
import random
import numpy as np

#Obtenemos los datos
def get_data_from_sql():
    """Lee matrices desde Oracle y las prepara para OR-Tools."""
    
    conn_config = ConfiguracionConexion(config_id="DWRAC", ruta='config_acceso.yaml')
    db = AccessDB(conn_config)
    
    # Traemos distancias
    TABLA_RUTAS = "DWVEG_ORT.RMG_DIM_DISTANCIA"
    query = f"""
        SELECT LOC_ORIGEN, LOC_DESTINO, DISTANCIA_KM, TIEMPO_MIN, COMPATIBILIDAD_SN
        FROM {TABLA_RUTAS}
    """
    df = db.get_dataframe(query)
    
    # Penalización alta para caminos no compatibles o inexistentes
    PENALIZACION = 5000000

    if df.empty:
        raise Exception(f"La tabla {TABLA_RUTAS} está vacía.")

    all_nodes = pd.concat([df['LOC_ORIGEN'], df['LOC_DESTINO']]).unique()
    node_to_idx = {node: i for i, node in enumerate(all_nodes)}
    idx_to_node = {i: node for node, i in node_to_idx.items()}
    num_nodes = len(all_nodes)
    
    df['idx_origen'] = df['LOC_ORIGEN'].map(node_to_idx)
    df['idx_destino'] = df['LOC_DESTINO'].map(node_to_idx)
    
    # Aplicamos la restricción de compatibilidad
    df.loc[df['COMPATIBILIDAD_SN'] == 'N', ['DISTANCIA_KM', 'TIEMPO_MIN']] = PENALIZACION

    # Creamos las matrices asegurando que sean de tamaño N x N
    all_idxs = list(range(num_nodes))
    
    dist_pivot = df.pivot(index='idx_origen', columns='idx_destino', values='DISTANCIA_KM')
    dist_pivot = dist_pivot.reindex(index=all_idxs, columns=all_idxs, fill_value=PENALIZACION)
    dist_matrix = dist_pivot.fillna(PENALIZACION).values
    np.fill_diagonal(dist_matrix, 0) 
    dist_matrix = dist_matrix.round().astype(int).tolist()

    time_pivot = df.pivot(index='idx_origen', columns='idx_destino', values='TIEMPO_MIN')
    time_pivot = time_pivot.reindex(index=all_idxs, columns=all_idxs, fill_value=PENALIZACION)
    time_matrix = time_pivot.fillna(PENALIZACION).values
    np.fill_diagonal(time_matrix, 0) 
    time_matrix = time_matrix.round().astype(int).tolist()
    
    # Traemos coordenadas 
    TABLA_COORDS = "DWVEG_ORT.RMG_DIM_LOCALIZACION"  

    query_coords = f"""
        SELECT LOC_ID, LATITUD, LONGITUD
        FROM {TABLA_COORDS}
    """
    df_coords = db.get_dataframe(query_coords)

    node_coords = {}
    for _, row in df_coords.iterrows():
        node_id = row['LOC_ID']
        if node_id in node_to_idx:
            idx = node_to_idx[node_id]
            node_coords[idx] = (row['LATITUD'], row['LONGITUD'])
            
    return dist_matrix, time_matrix, node_to_idx, node_coords


def generate_map(data, manager, routing, solution):
    """Genera un mapa interactivo con iconos diferenciados para entregas y recogidas."""
    # Centramos el mapa en Sigüeiro
    depot_coords = data['node_coords'][data['depot']]
    m = folium.Map(location=depot_coords, zoom_start=12)

    # Colores para distinguir los vehículos
    colors = ['red', 'blue', 'green', 'purple', 'orange', 'darkred', 
              'cadetblue', 'darkpurple', 'pink', 'lightblue', 'black']

    for vehicle_id in range(data["num_vehicles"]):
        index = routing.Start(vehicle_id)
        route_coords = []
        color = colors[vehicle_id % len(colors)]
        
        while not routing.IsEnd(index):
            node_index = manager.IndexToNode(index)
            coords = data['node_coords'][node_index]
            route_coords.append(coords)
            
            #  ICONOS DIFERENCIADOS EN EL MAPA
            if node_index == data['depot']:
                icon_type = 'home'
                icon_color = 'black'
                label = "DEPÓSITO"
            elif node_index in data['delivery_nodes']:
                icon_type = 'arrow-down' 
                icon_color = color
                label = f"ENTREGA (Nodo {node_index})"
            elif node_index in data['pickup_nodes']:
                icon_type = 'arrow-up'   
                icon_color = color
                label = f"RECOGIDA (Nodo {node_index})"
            else:
                icon_type = 'ban' 
                icon_color = 'gray'
                label = f"Nodo {node_index}"

            # Añadir el marcador al mapa
            folium.Marker(
                location=coords, 
                popup=f"{label}<br>Vehículo: {vehicle_id}", 
                icon=folium.Icon(color=icon_color, icon=icon_type, prefix='fa') # Usamos FontAwesome
            ).add_to(m)
            
            index = solution.Value(routing.NextVar(index))
        
        # Añadir el regreso al depósito a la línea de la ruta
        node_index = manager.IndexToNode(index)
        route_coords.append(data['node_coords'][node_index])
        
        # Dibujar la línea de la ruta
        if len(route_coords) > 2:
            folium.PolyLine(
                route_coords, 
                color=color, 
                weight=3, 
                opacity=0.7,
                tooltip=f"Ruta Vehículo {vehicle_id}"
            ).add_to(m)

    # Guardar el mapa
    m.save("mapa_rutas.html")
    print("✅ Mapa generado con iconos diferenciados: abre 'mapa_rutas.html'")


def create_data_model():
    """Define los datos del problema."""
    data = {}
    
    dist_matrix, time_matrix, node_to_idx, node_coords = get_data_from_sql()
    
    #Estableciendo Sgüeiro como base
    NODO_BASE = "A00010"
    if NODO_BASE not in node_to_idx:
        raise Exception(f"CRÍTICO: El nodo base {NODO_BASE} no está en los datos.")
    indice_base = node_to_idx[NODO_BASE]

    #Numero de nodos
    num_nodes = len(dist_matrix)
    data['node_to_idx'] = node_to_idx

    #Matriz coordenadas de nodos
    data['node_coords'] = node_coords

    # Matriz de distancias entre nodos
    data["distance_matrix"] = dist_matrix

    # Matriz de tiempos
    data["time_matrix"] = time_matrix
    
    #Establecemos el nodo base
    data["depot"] = indice_base

    # Cantidad de carga a depositar en cada entrega.
    demands = [0] * num_nodes
    delivery_nodes = []
    pickup_nodes = []

    # Identificamos todos los nodos que NO son el depósito
    nodos_clientes = [i for i in range(num_nodes) if i != indice_base]
    mitad_clientes = len(nodos_clientes) // 2
    
    # Asignamos roles solo a clientes, nunca al depósito
    for i, nodo_idx in enumerate(nodos_clientes):
        if i < mitad_clientes:
            demands[nodo_idx] = -1          # ENTREGA: El camión suelta carga
            delivery_nodes.append(nodo_idx)
        else:
            demands[nodo_idx] = 1           # RECOGIDA: El camión suma carga
            pickup_nodes.append(nodo_idx)
   
    data["demands"] = demands
    # Definimos que nodos son las entregas y cuales son las recogidas
    data["delivery_nodes"] = delivery_nodes
    data["pickup_nodes"] = pickup_nodes

    # Capacidades de los vehículos
    data["num_vehicles"] = 15
    data["vehicle_capacities"] = [100] * data["num_vehicles"]
    
    # Ventanas de tiempo
    data["time_windows"] = [(0, 2000000)] * num_nodes
    
    return data

def print_solution(data, manager, routing, solution):
    """Imprime rutas, carga y tiempo de llegada a cada nodo."""
    total_distance = 0
    time_dimension = routing.GetDimensionOrDie("Time")
    capacity_dimension = routing.GetDimensionOrDie("Capacity")
    
    for vehicle_id in range(data["num_vehicles"]):
        index = routing.Start(vehicle_id)
        plan_output = f"Ruta vehículo {vehicle_id}:\n"
        route_distance = 0
        nodes_visited = 0
        
        while not routing.IsEnd(index):
            nodes_visited += 1
            node_index = manager.IndexToNode(index)
            load_var = solution.Value(capacity_dimension.CumulVar(index))
            time_var = solution.Value(time_dimension.CumulVar(index))
            
            plan_output += f"{node_index}(Carga={load_var}, Tiempo={time_var}) -> "
            
            previous_index = index
            index = solution.Value(routing.NextVar(index))
            route_distance += routing.GetArcCostForVehicle(previous_index, index, vehicle_id)
            
        load_var = solution.Value(capacity_dimension.CumulVar(index))
        time_var = solution.Value(time_dimension.CumulVar(index))
        plan_output += f"{manager.IndexToNode(index)}(Carga={load_var}, Tiempo={time_var})\n"
        plan_output += f"Distancia de la ruta: {route_distance}m\n"
        
        # Solo imprimimos si el vehículo salió del depósito (distancia > 0 o visitó más de 1 nodo)
        if nodes_visited > 1:
            print(plan_output)
            total_distance += route_distance
            
    print(f"Distancia total de todas las rutas: {total_distance}m")

def main():
    data = create_data_model()
    
    manager = pywrapcp.RoutingIndexManager(len(data["distance_matrix"]), data["num_vehicles"],
                                           data["depot"]
                                           )
    
    routing = pywrapcp.RoutingModel(manager)
    
    # Costo por distancia
    def distance_callback(from_index, to_index):
        return data["distance_matrix"][manager.IndexToNode(from_index)][manager.IndexToNode(to_index)]
    
    transit_callback_index = routing.RegisterTransitCallback(distance_callback)
    routing.SetArcCostEvaluatorOfAllVehicles(transit_callback_index)
    
    # Restricción de capacidad
    def demand_callback(from_index):
        return data["demands"][manager.IndexToNode(from_index)]
    
    demand_callback_index = routing.RegisterUnaryTransitCallback(demand_callback)
    routing.AddDimensionWithVehicleCapacity(
        demand_callback_index,
        0,  # null capacity slack
        data["vehicle_capacities"],  
        False,  # Debe ser False para permitir que el vehículo empiece con carga (SetValue)
        "Capacity"
    )
    
    capacity_dimension = routing.GetDimensionOrDie("Capacity")

    # Forzamos a cada vehículo a empezar LLENO
    for vehicle_id in range(data["num_vehicles"]):
        start_index = routing.Start(vehicle_id)
        capacity_dimension.CumulVar(start_index).SetValue(data["vehicle_capacities"][vehicle_id])

    # Restricción de tiempo
    def time_callback(from_index, to_index):
        return data["time_matrix"][manager.IndexToNode(from_index)][manager.IndexToNode(to_index)]
    
    time_callback_index = routing.RegisterTransitCallback(time_callback)
    
    routing.AddDimension(
        time_callback_index,
        1440,        # slack permitido
        50000000,   # tiempo máximo (Aumentado a 50M)
        False,
        "Time"
    )
    time_dimension = routing.GetDimensionOrDie("Time")
    
    # Aplicar ventanas de tiempo
    for node_index, (start, end) in enumerate(data["time_windows"]):
        index = manager.NodeToIndex(node_index)
        time_dimension.CumulVar(index).SetRange(start, end)

    #Forzar Entregas antes que Recogidas (Evitar Pickup -> Delivery)
    def pickup_count_callback(from_index):
        node = manager.IndexToNode(from_index)
        return 1 if node in data["pickup_nodes"] else 0

    pickup_count_index = routing.RegisterUnaryTransitCallback(pickup_count_callback)
    routing.AddDimension(pickup_count_index, 0, len(data["pickup_nodes"]) + 1, True, "PickupSequence")
    sequence_dimension = routing.GetDimensionOrDie("PickupSequence")

    for d in data["delivery_nodes"]:
        d_index = manager.NodeToIndex(d)
        sequence_dimension.CumulVar(d_index).SetMax(0)

    # Añadimos penalizaciones para permitir omitir nodos si son imposibles de alcanzar
    penalty = 100000
    for node in range(0, len(data["distance_matrix"])):
        if node != data["depot"]:
            routing.AddDisjunction([manager.NodeToIndex(node)], penalty)

    # Estrategia inicial de búsqueda
    search_parameters = pywrapcp.DefaultRoutingSearchParameters()
    

    search_parameters.first_solution_strategy = (
        routing_enums_pb2.FirstSolutionStrategy.PATH_MOST_CONSTRAINED_ARC)
    
    search_parameters.local_search_metaheuristic = (
        routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH)
    
    search_parameters.time_limit.seconds = 30
    
    solution = routing.SolveWithParameters(search_parameters)
    
    if solution:
        print("✅ SOLUCIÓN ENCONTRADA")
        print_solution(data, manager, routing, solution)
        generate_map(data, manager, routing, solution) 
    else:
        print("⚠ No se encontró solución.")
        
if __name__ == "__main__":
    main()