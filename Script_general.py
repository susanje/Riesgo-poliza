import numpy as np
import pandas as pd
import random
from cmath import nan
from sodapy import Socrata
import geopy
from geopy.geocoders import Nominatim
import datetime
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import csv
import logging
from tempfile import NamedTemporaryFile


default_args={
    'owner': 'keving91',
    'retries': 3,
    'retry_delay': timedelta(minutes= 5),
}


def extract_choques_nyc_from_api():
    client_choques_nyc = Socrata('data.cityofnewyork.us',
                  'P5yEsPDgBpkSlfs3G7Kg0DM6o',)
    results_choques_nyc = client_choques_nyc.get("h9gi-nx95", limit=800000)
    results_df_choques_nyc = pd.DataFrame.from_records(results_choques_nyc)
    return results_df_choques_nyc.to_csv("/home/kevin/airflow/dataset_dump/extracted_choques_nyc.csv")


def extract_robo_from_api():
    client_robo = Socrata('data.cityofnewyork.us',
                  'P5yEsPDgBpkSlfs3G7Kg0DM6o',)
    results_robo = client_robo.get("5uac-w243", limit=258000)
    results_df_robo = pd.DataFrame.from_records(results_robo)
    return results_df_robo.to_csv("/home/kevin/airflow/dataset_dump/extracted_robo.csv")


def extract_edad_from_api():
    client_edad = Socrata('data.cityofnewyork.us',
                  'P5yEsPDgBpkSlfs3G7Kg0DM6o',)
    results_edad = client_edad.get("bm4k-52h4", limit=1000000)
    results_df_edad = pd.DataFrame.from_records(results_edad)
    return results_df_edad.to_csv("/home/kevin/airflow/dataset_dump/extracted_edad.csv")


def extract_edad_2_from_api():
    client_edad = Socrata('data.cityofnewyork.us',
                  'P5yEsPDgBpkSlfs3G7Kg0DM6o',)
    results_edad_2 = client_edad.get("f55k-p6yu", limit=1000000)
    results_df_edad_2 = pd.DataFrame.from_records(results_edad_2)
    return results_df_edad_2.to_csv("/home/kevin/airflow/dataset_dump/extracted_edad_2.csv")


def transform_main():
    main_df = pd.read_csv("/home/kevin/airflow/dataset_dump/extracted_choques_nyc.csv")
    main_df['crash_date'].replace({'T00:00:00.000':''}, regex=True, inplace=True)
    main_df['crash_date_time']=pd.to_datetime(main_df['crash_date'] + main_df['crash_time'], format='%Y-%m-%d%H:%M')
    choques_nyc=main_df[main_df['crash_date_time'].dt.year.isin([2019,2020,2021,2022])]
    choques_nyc.rename(columns= {'crash_date_time': 'colision_fecha_hora',
        'crash_date': 'colision_fecha',
        'crash_time': 'colision_hora',
        'on_street_name': 'nombre_de_calle',
        'off_street_name': 'nombre_fuera_de_calle',
        'cross_street_name': 'calle_interseccion',
        'number_of_persons_injured': 'numero_de_personas_heridas',
        'number_of_persons_killed': 'numero_de_personas_fallecidas',
        'number_of_pedestrians_injured': 'numero_de_peatones_heridos',
        'number_of_pedestrians_killed': 'numero_de_peatones_fallecidos',
        'number_of_cyclist_injured': 'numero_de_ciclistas_heridos',
        'number_of_cyclist_killed': 'numero_de_ciclistas_fallecidos',
        'number_of_motorist_injured': 'numero_de_conductores_heridos',
        'number_of_motorist_killed': 'numero_de_conductores_fallecidos',
        'contributing_factor_vehicle_1': 'factor_contribuyente_vehiculo_1',
        'contributing_factor_vehicle_2': 'factor_contribuyente_vehiculo_2',
        'collision_id': 'colision_ID',
        'vehicle_type_code1': 'codigo_tipo_de_vehiculo_1',
        'vehicle_type_code2': 'codigo_tipo_de_vehiculo_2',
        'borough': 'localidad',
        'zip_code': 'codigo_postal',
        'latitude': 'latitud',
        'longitude': 'longitud',
        'location': 'ubicacion',
        'contributing_factor_vehicle_3': 'factor_contribuyente_vehiculo_3',
        'vehicle_type_code_3': 'codigo_tipo_de_vehiculo_3',
        'contributing_factor_vehicle_4': 'factor_contribuyente_vehiculo_4',
        'vehicle_type_code_4': 'codigo_tipo_de_vehiculo_4',
        'contributing_factor_vehicle_5': 'factor_contribuyente_vehiculo_5',
        'vehicle_type_code_5': 'codigo_tipo_de_vehiculo_5',
        'day': 'dia',
        'no_location': 'sin_ubicacion',
        'day_time': 'hora_del_dia'}, inplace= True)
    choques_nyc['anio'] = choques_nyc['colision_fecha_hora'].dt.year
    choques_nyc['mes'] = choques_nyc['colision_fecha_hora'].dt.month
    choques_nyc['dia'] = choques_nyc['colision_fecha_hora'].dt.day
    choques_nyc['hora'] = choques_nyc['colision_fecha_hora'].dt.time
    codigo_tipo_de_vehiculo_1 = list(choques_nyc.codigo_tipo_de_vehiculo_1)
    codigo_tipo_de_vehiculo_1 = [str(v) for v in codigo_tipo_de_vehiculo_1]
    codigo_tipo_de_vehiculo_2 = list(choques_nyc.codigo_tipo_de_vehiculo_2)
    codigo_tipo_de_vehiculo_2 = [str(v) for v in codigo_tipo_de_vehiculo_2]
    codigo_tipo_de_vehiculo_3 = list(choques_nyc.codigo_tipo_de_vehiculo_3)
    codigo_tipo_de_vehiculo_3 = [str(v) for v in codigo_tipo_de_vehiculo_3]
    codigo_tipo_de_vehiculo_4 = list(choques_nyc.codigo_tipo_de_vehiculo_4)
    codigo_tipo_de_vehiculo_4 = [str(v) for v in codigo_tipo_de_vehiculo_4]
    codigo_tipo_de_vehiculo_5 = list(choques_nyc.codigo_tipo_de_vehiculo_5)
    codigo_tipo_de_vehiculo_5 = [str(v) for v in codigo_tipo_de_vehiculo_5]
    codigo_tipo_de_vehiculo_1 = [w.title() for w in codigo_tipo_de_vehiculo_1]
    codigo_tipo_de_vehiculo_2 = [w.title() for w in codigo_tipo_de_vehiculo_2]
    codigo_tipo_de_vehiculo_3 = [w.title() for w in codigo_tipo_de_vehiculo_3]
    codigo_tipo_de_vehiculo_4 = [w.title() for w in codigo_tipo_de_vehiculo_4]
    codigo_tipo_de_vehiculo_5 = [w.title() for w in codigo_tipo_de_vehiculo_5]
    good_ones = ['Sedan', 'Station Wagon/Sport Utility Vehicle', 'Taxi', 'Pick-Up Truck', 'Box Truck', 'Bike', 'Bus', 'Tractor Truck Diesel', 'Motorcycle', 'Van', 'E-Bike', 'Ambulance', 'E-Scooter', 'Dump', 'Convertible', 'Flat Bed', 'Moped', 'Garbage Or Refuse']
    vehiculo_1= [x for x in codigo_tipo_de_vehiculo_1]
    for v in range(0, len(vehiculo_1)):
        if vehiculo_1[v] == 'Nan':
            vehiculo_1[v] = 'Unknown'
        elif vehiculo_1[v] not in good_ones:
            vehiculo_1[v] = random.choice(good_ones)
    vehiculo_2= [x for x in codigo_tipo_de_vehiculo_2]
    for v in range(0, len(vehiculo_2)):
        if vehiculo_2[v] == 'Nan':
            vehiculo_2[v] = 'Unknown'
        elif vehiculo_2[v] not in good_ones:
            vehiculo_2[v] = random.choice(good_ones)
    vehiculo_3= [x for x in codigo_tipo_de_vehiculo_3]
    for v in range(0, len(vehiculo_3)):
        if vehiculo_3[v] == 'Nan':
            vehiculo_3[v] = 'Unknown'
        elif vehiculo_3[v] not in good_ones:
            vehiculo_3[v] = random.choice(good_ones)
    vehiculo_4= [x for x in codigo_tipo_de_vehiculo_4]
    for v in range(0, len(vehiculo_4)):
        if vehiculo_4[v] == 'Nan':
            vehiculo_4[v] = 'Unknown'
        elif vehiculo_4[v] not in good_ones:
            vehiculo_4[v] = random.choice(good_ones)
    vehiculo_5= [x for x in codigo_tipo_de_vehiculo_5]
    for v in range(0, len(vehiculo_5)):
        if vehiculo_5[v] == 'Nan':
            vehiculo_5[v] = 'Unknown'
        elif vehiculo_5[v] not in good_ones:
            vehiculo_5[v] = random.choice(good_ones)
    choques_nyc['codigo_tipo_de_vehiculo_1'] = vehiculo_1
    choques_nyc['codigo_tipo_de_vehiculo_2'] = vehiculo_2
    choques_nyc['codigo_tipo_de_vehiculo_3'] = vehiculo_3
    choques_nyc['codigo_tipo_de_vehiculo_4'] = vehiculo_4
    choques_nyc['codigo_tipo_de_vehiculo_5'] = vehiculo_5
    choques_nyc.factor_contribuyente_vehiculo_1.fillna('Sin Especificar', inplace= True)
    choques_nyc.factor_contribuyente_vehiculo_2.fillna('Sin Especificar', inplace= True)
    choques_nyc.factor_contribuyente_vehiculo_3.fillna('Sin Especificar', inplace= True)
    choques_nyc.factor_contribuyente_vehiculo_4.fillna('Sin Especificar', inplace= True)
    choques_nyc.factor_contribuyente_vehiculo_5.fillna('Sin Especificar', inplace= True)
    choques_nyc.factor_contribuyente_vehiculo_1.replace(['Following Too Closely',
        'Unspecified',
        'Turning Improperly',
        'Pavement Slippery',
        'Driver Inattention/Distraction',
        'Other Vehicular',
        'Passing Too Closely',
        'Passing or Lane Usage Improper',
        'Driver Inexperience',
        'Failure to Yield Right-of-Way',
        'Brakes Defective',
        'Unsafe Speed',
        'Backing Unsafely',
        'Reaction to Uninvolved Vehicle',
        'View Obstructed/Limited',
        'Steering Failure',
        'Traffic Control Disregarded',
        'Drugs (illegal)',
        'Aggressive Driving/Road Rage',
        'Fell Asleep',
        'Pedestrian/Bicyclist/Other Pedestrian Error/Confusion',
        'Alcohol Involvement',
        'Unsafe Lane Changing',
        'Pavement Defective',
        'Other Lighting Defects',
        'Oversized Vehicle',
        'Animals Action',
        'Outside Car Distraction',
        'Illnes',
        'Driverless/Runaway Vehicle',
        'Passenger Distraction',
        'Tire Failure/Inadequate',
        'Fatigued/Drowsy',
        'Glare',
        'Cell Phone (hand-Held)',
        'Obstruction/Debris',
        'Shoulders Defective/Improper',
        'Lost Consciousness',
        'Accelerator Defective',
        'Failure to Keep Right',
        'Traffic Control Device Improper/Non-Working',
        'Eating or Drinking',
        'Vehicle Vandalism',
        'Physical Disability',
        'Cell Phone (hands-free)',
        'Lane Marking Improper/Inadequate',
        'Other Electronic Device',
        'Using On Board Navigation Device',
        'Tow Hitch Defective',
        'Texting',
        'Headlights Defective',
        'Tinted Windows',
        'Prescription Medication',
        'Listening/Using Headphones',
        'Windshield Inadequate'],
        ['Seguir Muy De Cerca',
        'Sin Especificar',
        'Giro Inapropiado',
        'Pavimento Resbaladizo',
        'Conductor Inatento/Distraido',
        'Otra Vehicular',
        'Pasar Muy De Cerca',
        'Pasar O Usar carril Inapropiadamente',
        'Inexperiencia Del Conductor',
        'No Otorgar Derecho De Paso',
        'Frenos Defectuosos',
        'Velocidad Peligrosa',
        'Reversa Peligrosa',
        'Reaccion A Vehiculo No Involucrado',
        'Vision Obstruida/Limitada',
        'Falla Al Doblar',
        'Ignorar Control De Trafico',
        'Drogas (ilegales)',
        'Conduccion Agresiva',
        'Quedarse Dormido',
        'Peaton/Ciclista/Otro Error De Peaton/Confusion',
        'Alcohol Involucrado',
        'Cambio de carril peligroso',
        'Pavimento defectuoso',
        'Otros Defectos De Iluminacion',
        'Vehiculo Demasiado Grande',
        'Accion Animal',
        'Distraccion De Auto Ajeno',
        'Enfermedad',
        'Vehiculo Sin Conductor',
        'Distraccion De Pasajero',
        'Falla/Defecto de neumatico',
        'Fatiga/Adormecimiento',
        'Encandilamiento',
        'Celular(En Mano)',
        'Obstruccion/Escombros',
        'Guardabarro Defectivo/Inapropiado',
        'Perdida De Conciencia',
        'Acelerador Defectuoso',
        'Fallo De Mantener En Linear Recta',
        'Dispositivo De Control De Trafico Roto/Inapropiado',
        'Comida O Bebida',
        'Vandalismo De Vehiculo',
        'Discapacidad Fisica',
        'Celular(Manos Libres)',
        'Delimitacion de Carrilo Inadecuado',
        'Otro Dispositivo Electronico',
        'Usar Dispositivo De Navigacion De A Bordo',
        'Enganche De Remolque Defectuoso',
        'Mensajeria',
        'Luces Defectivas',
        'Vidrios Polarizados',
        'Prescripcion Medica',
        'Usar Auriculares',
        'Parabrisas Inadecuados'], inplace= True)
    choques_nyc.factor_contribuyente_vehiculo_2.replace(['Following Too Closely',
        'Unspecified',
        'Turning Improperly',
        'Pavement Slippery',
        'Driver Inattention/Distraction',
        'Other Vehicular',
        'Passing Too Closely',
        'Passing or Lane Usage Improper',
        'Driver Inexperience',
        'Failure to Yield Right-of-Way',
        'Brakes Defective',
        'Unsafe Speed',
        'Backing Unsafely',
        'Reaction to Uninvolved Vehicle',
        'View Obstructed/Limited',
        'Steering Failure',
        'Traffic Control Disregarded',
        'Drugs (illegal)',
        'Aggressive Driving/Road Rage',
        'Fell Asleep',
        'Pedestrian/Bicyclist/Other Pedestrian Error/Confusion',
        'Alcohol Involvement',
        'Unsafe Lane Changing',
        'Pavement Defective',
        'Other Lighting Defects',
        'Oversized Vehicle',
        'Animals Action',
        'Outside Car Distraction',
        'Illnes',
        'Driverless/Runaway Vehicle',
        'Passenger Distraction',
        'Tire Failure/Inadequate',
        'Fatigued/Drowsy',
        'Glare',
        'Cell Phone (hand-Held)',
        'Obstruction/Debris',
        'Shoulders Defective/Improper',
        'Lost Consciousness',
        'Accelerator Defective',
        'Failure to Keep Right',
        'Traffic Control Device Improper/Non-Working',
        'Eating or Drinking',
        'Vehicle Vandalism',
        'Physical Disability',
        'Cell Phone (hands-free)',
        'Lane Marking Improper/Inadequate',
        'Other Electronic Device',
        'Using On Board Navigation Device',
        'Tow Hitch Defective',
        'Texting',
        'Headlights Defective',
        'Tinted Windows',
        'Prescription Medication',
        'Listening/Using Headphones'],
        ['Seguir Muy De Cerca',
        'Sin Especificar',
        'Giro Inapropiado',
        'Pavimento Resbaladizo',
        'Conductor Inatento/Distraido',
        'Otra Vehicular',
        'Pasar Muy De Cerca',
        'Pasar O Usar carril Inapropiadamente',
        'Inexperiencia Del Conductor',
        'No Otorgar Derecho De Paso',
        'Frenos Defectuosos',
        'Velocidad Peligrosa',
        'Reversa Peligrosa',
        'Reaccion A Vehiculo No Involucrado',
        'Vision Obstruida/Limitada',
        'Falla Al Doblar',
        'Ignorar Control De Trafico',
        'Drogas (ilegales)',
        'Conduccion Agresiva',
        'Quedarse Dormido',
        'Peaton/Ciclista/Otro Error De Peaton/Confusion',
        'Alcohol Involucrado',
        'Cambio de carril peligroso',
        'Pavimento defectuoso',
        'Otros Defectos De Iluminacion',
        'Vehiculo Demasiado Grande',
        'Accion Animal',
        'Distraccion De Auto Ajeno',
        'Enfermedad',
        'Vehiculo Sin Conductor',
        'Distraccion De Pasajero',
        'Falla/Defecto de neumatico',
        'Fatiga/Adormecimiento',
        'Encandilamiento',
        'Celular(En Mano)',
        'Obstruccion/Escombros',
        'Guardabarro Defectivo/Inapropiado',
        'Perdida De Conciencia',
        'Acelerador Defectuoso',
        'Fallo De Mantener En Linea Recta',
        'Dispositivo De Control De Trafico Roto/Inapropiado',
        'Comida O Bebida',
        'Vandalismo De Vehiculo',
        'Discapacidad Fisica',
        'Celular(Manos Libres)',
        'Delimitacion de Carrilo Inadecuado',
        'Otro Dispositivo Electronico',
        'Usar Dispositivo De Navigacion De A Bordo',
        'Enganche De Remolque Defectuoso',
        'Mensajeria',
        'Luces Defectivas',
        'Vidrios Polarizados',
        'Prescripcion Medica',
        'Usar Auriculares'], inplace= True)
    choques_nyc.factor_contribuyente_vehiculo_3.replace(['Following Too Closely',
        'Unspecified',
        'Turning Improperly',
        'Pavement Slippery',
        'Driver Inattention/Distraction',
        'Other Vehicular',
        'Passing Too Closely',
        'Passing or Lane Usage Improper',
        'Driver Inexperience',
        'Failure to Yield Right-of-Way',
        'Brakes Defective',
        'Unsafe Speed',
        'Backing Unsafely',
        'Reaction to Uninvolved Vehicle',
        'View Obstructed/Limited',
        'Steering Failure',
        'Traffic Control Disregarded',
        'Drugs (illegal)',
        'Aggressive Driving/Road Rage',
        'Fell Asleep',
        'Pedestrian/Bicyclist/Other Pedestrian Error/Confusion',
        'Alcohol Involvement',
        'Unsafe Lane Changing',
        'Pavement Defective',
        'Oversized Vehicle',
        'Outside Car Distraction',
        'Illnes',
        'Driverless/Runaway Vehicle',
        'Passenger Distraction',
        'Tire Failure/Inadequate',
        'Glare',
        'Obstruction/Debris',
        'Lost Consciousness',
        'Failure to Keep Right',
        'Traffic Control Device Improper/Non-Working',
        'Eating or Drinking',
        'Cell Phone (hands-free)',
        'Other Electronic Device'],
        ['Seguir Muy De Cerca',
        'Sin Especificar',
        'Giro Inapropiado',
        'Pavimento Resbaladizo',
        'Conductor Inatento/Distraido',
        'Otra Vehicular',
        'Pasar Muy De Cerca',
        'Pasar O Usar carril Inapropiadamente',
        'Inexperiencia Del Conductor',
        'No Otorgar Derecho De Paso',
        'Frenos Defectuosos',
        'Velocidad Peligrosa',
        'Reversa Peligrosa',
        'Reaccion A Vehiculo No Involucrado',
        'Vision Obstruida/Limitada',
        'Falla Al Doblar',
        'Ignorar Control De Trafico',
        'Drogas (ilegales)',
        'Conduccion Agresiva',
        'Quedarse Dormido',
        'Peaton/Ciclista/Otro Error De Peaton/Confusion',
        'Alcohol Involucrado',
        'Cambio de carril peligroso',
        'Pavimento defectuoso',
        'Vehiculo Demasiado Grande',
        'Distraccion De Auto Ajeno',
        'Enfermedad',
        'Vehiculo Sin Conductor',
        'Distraccion De Pasajero',
        'Falla/Defecto de neumatico',
        'Encandilamiento',
        'Obstruccion/Escombros',
        'Perdida De Conciencia',
        'Fallo De Mantener En Linear Recta',
        'Dispositivo De Control De Trafico Roto/Inapropiado',
        'Comida O Bebida',
        'Celular(Manos Libres)',
        'Otro Dispositivo Electronico'], inplace= True)
    choques_nyc.factor_contribuyente_vehiculo_4.replace(['Following Too Closely',
        'Unspecified',
        'Pavement Slippery',
        'Driver Inattention/Distraction',
        'Other Vehicular',
        'Passing Too Closely',
        'Passing or Lane Usage Improper',
        'Driver Inexperience',
        'Failure to Yield Right-of-Way',
        'Brakes Defective',
        'Unsafe Speed',
        'Backing Unsafely',
        'Reaction to Uninvolved Vehicle',
        'View Obstructed/Limited',
        'Traffic Control Disregarded',
        'Aggressive Driving/Road Rage',
        'Fell Asleep',
        'Alcohol Involvement',
        'Unsafe Lane Changing',
        'Pavement Defective',
        'Outside Car Distraction',
        'Obstruction/Debris'],
        ['Seguir Muy De Cerca',
        'Sin Especificar',
        'Pavimento Resbaladizo',
        'Conductor Inatento/Distraido',
        'Otra Vehicular',
        'Pasar Muy De Cerca',
        'Pasar O Usar carril Inapropiadamente',
        'Inexperiencia Del Conductor',
        'No Otorgar Derecho De Paso',
        'Frenos Defectuosos',
        'Velocidad Peligrosa',
        'Reversa Peligrosa',
        'Reaccion A Vehiculo No Involucrado',
        'Vision Obstruida/Limitada',
        'Ignorar Control De Trafico',
        'Conduccion Agresiva',
        'Quedarse Dormido',
        'Alcohol Involucrado',
        'Cambio de carril peligroso',
        'Pavimento defectuoso',
        'Distraccion De Auto Ajeno',
        'Obstruccion/Escombros'], inplace= True)
    choques_nyc.factor_contribuyente_vehiculo_5.replace(['Following Too Closely',
        'Unspecified',
        'Pavement Slippery',
        'Driver Inattention/Distraction',
        'Other Vehicular',
        'Passing Too Closely',
        'Driver Inexperience',
        'Failure to Yield Right-of-Way',
        'Unsafe Speed',
        'Reaction to Uninvolved Vehicle',
        'Alcohol Involvement',
        'Pavement Defective',
        'Outside Car Distraction',
        'Obstruction/Debris'],
        ['Seguir Muy De Cerca',
        'Sin Especificar',
        'Pavimento Resbaladizo',
        'Conductor Inatento/Distraido',
        'Otra Vehicular',
        'Pasar Muy De Cerca',
        'Inexperiencia Del Conductor',
        'No Otorgar Derecho De Paso',
        'Velocidad Peligrosa',
        'Reaccion A Vehiculo No Involucrado',
        'Alcohol Involucrado',
        'Pavimento defectuoso',
        'Distraccion De Auto Ajeno',
        'Obstruccion/Escombros'], inplace= True)
    choques_nyc.codigo_tipo_de_vehiculo_1.replace(
        ['Station Wagon/Sport Utility Vehicle',
        'Pick-Up Truck',
        'Box Truck',
        'Bus',
        'Unknown',
        'Ambulance',
        'Bike',
        'Van',
        'Flat Bed',
        'Moped',
        'Tractor Truck Diesel',
        'Dump',
        'Motorcycle',
        'Garbage Or Refuse'],
        ['Camioneta/Vehiculo Utilitario Deportivo',
        'Camioneta',
        'Camion De Caja',
        'Autobus',
        'Desconocido',
        'Ambulancia',
        'Motocicleta',
        'Furgoneta',
        'Camion',
        'Motocicleta',
        'Tractor',
        'Camion De Basura',
        'Motocicleta',
        'Camion De Basura'], inplace= True)
    choques_nyc.codigo_tipo_de_vehiculo_2.replace(
        ['Station Wagon/Sport Utility Vehicle',
        'Pick-Up Truck',
        'Box Truck',
        'Bus',
        'Unknown',
        'Ambulance',
        'Bike',
        'Van',
        'Flat Bed',
        'Moped',
        'Tractor Truck Diesel',
        'Dump',
        'Motorcycle',
        'Garbage Or Refuse'],
        ['Camioneta/Vehiculo Utilitario Deportivo',
        'Camioneta',
        'Camion De Caja',
        'Autobus',
        'Desconocido',
        'Ambulancia',
        'Motocicleta',
        'Furgoneta',
        'Camion',
        'Motocicleta',
        'Tractor',
        'Camion De Basura',
        'Motocicleta',
        'Camion De Basura'], inplace= True)
    choques_nyc.codigo_tipo_de_vehiculo_3.replace(
        ['Station Wagon/Sport Utility Vehicle',
        'Pick-Up Truck',
        'Box Truck',
        'Bus',
        'Unknown',
        'Ambulance',
        'Bike',
        'Van',
        'Flat Bed',
        'Moped',
        'Tractor Truck Diesel',
        'Dump',
        'Motorcycle',
        'Garbage Or Refuse'],
        ['Camioneta/Vehiculo Utilitario Deportivo',
        'Camioneta',
        'Camion De Caja',
        'Autobus',
        'Desconocido',
        'Ambulancia',
        'Motocicleta',
        'Furgoneta',
        'Camion',
        'Motocicleta',
        'Tractor',
        'Camion De Basura',
        'Motocicleta',
        'Camion De Basura'], inplace= True)
    choques_nyc.codigo_tipo_de_vehiculo_4.replace(
        ['Station Wagon/Sport Utility Vehicle',
        'Pick-Up Truck',
        'Box Truck',
        'Bus',
        'Unknown',
        'Ambulance',
        'Bike',
        'Van',
        'Flat Bed',
        'Moped',
        'Tractor Truck Diesel',
        'Dump',
        'Motorcycle',
        'Garbage Or Refuse'],
        ['Camioneta/Vehiculo Utilitario Deportivo',
        'Camioneta',
        'Camion De Caja',
        'Autobus',
        'Desconocido',
        'Ambulancia',
        'Motocicleta',
        'Furgoneta',
        'Camion',
        'Motocicleta',
        'Tractor',
        'Camion De Basura',
        'Motocicleta',
        'Camion De Basura'], inplace= True)
    choques_nyc.codigo_tipo_de_vehiculo_5.replace(
        ['Station Wagon/Sport Utility Vehicle',
        'Pick-Up Truck',
        'Box Truck',
        'Bus',
        'Unknown',
        'Ambulance',
        'Bike',
        'Van',
        'Flat Bed',
        'Moped',
        'Tractor Truck Diesel',
        'Dump',
        'Motorcycle',
        'Garbage Or Refuse'],
        ['Camioneta/Vehiculo Utilitario Deportivo',
        'Camioneta',
        'Camion De Caja',
        'Autobus',
        'Desconocido',
        'Ambulancia',
        'Motocicleta',
        'Furgoneta',
        'Camion',
        'Motocicleta',
        'Tractor',
        'Camion De Basura',
        'Motocicleta',
        'Camion De Basura'], inplace= True)
    choques_nyc['tipo_causante']= choques_nyc['codigo_tipo_de_vehiculo_1']
    choques_nyc.loc[(choques_nyc.codigo_tipo_de_vehiculo_1 != 'Desconocido'),'codigo_tipo_de_vehiculo_1']=1
    choques_nyc.loc[(choques_nyc.codigo_tipo_de_vehiculo_2 != 'Desconocido'),'codigo_tipo_de_vehiculo_2']=1
    choques_nyc.loc[(choques_nyc.codigo_tipo_de_vehiculo_3 != 'Desconocido'),'codigo_tipo_de_vehiculo_3']=1
    choques_nyc.loc[(choques_nyc.codigo_tipo_de_vehiculo_4 != 'Desconocido'),'codigo_tipo_de_vehiculo_4']=1
    choques_nyc.loc[(choques_nyc.codigo_tipo_de_vehiculo_5 != 'Desconocido'),'codigo_tipo_de_vehiculo_5']=1
    choques_nyc.loc[(choques_nyc.codigo_tipo_de_vehiculo_1 == 'Desconocido'),'codigo_tipo_de_vehiculo_1']=0
    choques_nyc.loc[(choques_nyc.codigo_tipo_de_vehiculo_2 == 'Desconocido'),'codigo_tipo_de_vehiculo_2']=0
    choques_nyc.loc[(choques_nyc.codigo_tipo_de_vehiculo_3 == 'Desconocido'),'codigo_tipo_de_vehiculo_3']=0
    choques_nyc.loc[(choques_nyc.codigo_tipo_de_vehiculo_4 == 'Desconocido'),'codigo_tipo_de_vehiculo_4']=0
    choques_nyc.loc[(choques_nyc.codigo_tipo_de_vehiculo_5 == 'Desconocido'),'codigo_tipo_de_vehiculo_5']=0
    choques_nyc['cantidad_autos_choque']= choques_nyc['codigo_tipo_de_vehiculo_1']+choques_nyc['codigo_tipo_de_vehiculo_2'] + choques_nyc['codigo_tipo_de_vehiculo_3'] + choques_nyc['codigo_tipo_de_vehiculo_4'] + choques_nyc['codigo_tipo_de_vehiculo_5']
    choques_nyc['numero_de_personas_heridas'].fillna(0, inplace=True)
    choques_nyc['numero_de_personas_fallecidas'].fillna(0, inplace=True)
    choques_nyc['numero_de_peatones_heridos'].fillna(0, inplace=True)
    choques_nyc['numero_de_peatones_fallecidos'].fillna(0, inplace=True)
    choques_nyc['numero_de_ciclistas_heridos'].fillna(0, inplace=True)
    choques_nyc['numero_de_ciclistas_fallecidos'].fillna(0, inplace=True)
    choques_nyc['numero_de_conductores_heridos'].fillna(0, inplace=True)
    choques_nyc['numero_de_conductores_fallecidos'].fillna(0, inplace=True)
    choques_nyc['numero_de_personas_heridas']=choques_nyc['numero_de_personas_heridas'].astype(int)
    choques_nyc['numero_de_personas_fallecidas']= choques_nyc['numero_de_personas_fallecidas'].astype(int)
    choques_nyc['numero_de_peatones_heridos']= choques_nyc['numero_de_peatones_heridos'].astype(int)
    choques_nyc['numero_de_peatones_fallecidos']= choques_nyc['numero_de_peatones_fallecidos'].astype(int)
    choques_nyc['numero_de_ciclistas_heridos']= choques_nyc['numero_de_ciclistas_heridos'].astype(int)
    choques_nyc['numero_de_ciclistas_fallecidos']= choques_nyc['numero_de_ciclistas_fallecidos'].astype(int)
    choques_nyc['numero_de_conductores_heridos']= choques_nyc['numero_de_conductores_heridos'].astype(int)
    choques_nyc['numero_de_conductores_fallecidos']= choques_nyc['numero_de_conductores_fallecidos'].astype(int)
    choques_nyc['numero_de_personas_fallecidas'] = choques_nyc['numero_de_personas_fallecidas'].apply(lambda x: x*2)
    choques_nyc['numero_de_peatones_fallecidos'] = choques_nyc['numero_de_peatones_fallecidos'].apply(lambda x: x*2)
    choques_nyc['numero_de_ciclistas_fallecidos'] = choques_nyc['numero_de_ciclistas_fallecidos'].apply(lambda x: x*2)
    choques_nyc['numero_de_conductores_fallecidos'] = choques_nyc['numero_de_conductores_fallecidos'].apply(lambda x: x*2)
    columnas=['numero_de_personas_fallecidas', 'numero_de_peatones_fallecidos', 'numero_de_ciclistas_fallecidos', 'numero_de_conductores_fallecidos', 'numero_de_personas_heridas', 'numero_de_peatones_heridos', 'numero_de_ciclistas_heridos', 'numero_de_conductores_heridos']
    choques_nyc['letalidad']= choques_nyc[columnas].sum(axis=1)
    choques_nyc['latitud']= choques_nyc['latitud'].astype(float)
    choques_nyc['longitud']= choques_nyc['longitud'].astype(float)
    choques_nyc=choques_nyc.dropna(subset=['latitud', 'longitud']).reset_index(drop=True)
    prototipo= pd.DataFrame().assign(id_choque=choques_nyc['colision_ID'],fecha_y_hora= choques_nyc['colision_fecha_hora'],anio= choques_nyc['anio'],mes=choques_nyc['mes'],dia=choques_nyc['dia'],latitud=choques_nyc['latitud'], longitud=choques_nyc['longitud'], auto_causante=choques_nyc['tipo_causante'],localidad=choques_nyc['localidad'],codigo_potal=choques_nyc['codigo_postal'], cantidad_autos_choque=choques_nyc['cantidad_autos_choque'],letalidad=choques_nyc['letalidad'] )
    prototipo = prototipo.set_index('id_choque')
    principal_sql=prototipo 
    principal_sql.drop('fecha_y_hora', axis=1, inplace=True)
    prototipo['auto_causante']=prototipo['auto_causante'].str.replace('[^\w\s]', '')
    prototipo.to_csv("/home/kevin/airflow/dataset_dump/principal_sql.csv")


def transform_robo():
    results_df_robo= pd.read_csv("/home/kevin/airflow/dataset_dump/extracted_robo.csv")
    results_df_robo['cmplnt_to_dt'].replace({'T00:00:00.000':''}, regex=True, inplace=True)
    results_df_robo['fecha_y_hora']=pd.to_datetime(results_df_robo['cmplnt_to_dt'] + results_df_robo['cmplnt_fr_tm'], format='%Y-%m-%d%H:%M:%S')
    crimen=results_df_robo[results_df_robo['fecha_y_hora'].dt.year.isin([2019,2020,2021,2022])]
    robo= pd.DataFrame().assign(fecha_y_hora= crimen['fecha_y_hora'], delito= crimen['pd_desc'], localidad= crimen['boro_nm'],latitud= crimen['latitude'],longitud= crimen['longitude'],)
    robo['anio'] = robo['fecha_y_hora'].dt.year
    robo['mes'] = robo['fecha_y_hora'].dt.month
    robo['dia'] = robo['fecha_y_hora'].dt.day
    robo['hora'] = robo['fecha_y_hora'].dt.time
    robo= robo.drop(robo[robo.delito != 'ROBBERY,CAR JACKING'].index).reset_index(drop=True)
    robo_sql=robo
    robo_sql.drop('fecha_y_hora', axis=1, inplace=True)
    robo_sql.drop('hora', axis=1, inplace=True)
    return robo_sql.to_csv("/home/kevin/airflow/dataset_dump/robo_sql.csv")

def transform_edad():
    results_df_edad= pd.read_csv("/home/kevin/airflow/dataset_dump/extracted_edad.csv")
    results_df_edad['crash_date'].replace({'T00:00:00.000':''}, regex=True, inplace=True)
    results_df_edad['crash_date_time']=pd.to_datetime(results_df_edad['crash_date'] + results_df_edad['crash_time'], format='%Y-%m-%d%H:%M')
    choques_df=results_df_edad[results_df_edad['crash_date_time'].dt.year.isin([2019,2020,2021,2022])].reset_index(drop=True)
    return choques_df.to_csv("/home/kevin/airflow/dataset_dump/choques.csv")

def transform_edad_2():
    results_df_edad_2= pd.read_csv("/home/kevin/airflow/dataset_dump/extracted_edad_2.csv")
    results_df_edad_2['crash_date'].replace({'T00:00:00.000':''}, regex=True, inplace=True)
    results_df_edad_2['crash_date_time']=pd.to_datetime(results_df_edad_2['crash_date'] + results_df_edad_2['crash_time'], format='%Y-%m-%d%H:%M')
    personas_df=results_df_edad_2[results_df_edad_2['crash_date_time'].dt.year.isin([2019,2020,2021,2022])].reset_index(drop=True)
    edad = pd.DataFrame().assign(id_choque=personas_df['collision_id'], edad=personas_df['person_age'])
    edad=edad.dropna(subset=['edad']).reset_index(drop=True)
    edad['edad'] = edad['edad'].astype(int)
    edad = edad.drop(edad[edad.edad > 80].index).reset_index(drop=True)
    edad = edad.drop(edad[edad.edad < 16].index).reset_index(drop=True)
    bins= [0,16,22,36,51,102]
    labels = ['menor','adolescente','adulto_joven','adulto','adulto_mayor']
    edad['rango_etario'] = pd.cut(edad['edad'], bins=bins, labels=labels, right=False)
    return edad.to_csv("/home/kevin/airflow/dataset_dump/edad_sql.csv")


def transform_causa():
    choques_df= pd.read_csv("/home/kevin/airflow/dataset_dump/choques.csv")
    causa_df = pd.DataFrame().assign(id_choque=choques_df['collision_id'], fecha_hora=choques_df['crash_date_time'], sexo=choques_df['driver_sex'],anio_del_vehiculo=choques_df['vehicle_year'])
    causa_df['fecha_hora']= pd.to_datetime(causa_df['fecha_hora'])
    causa_df['anio'] = causa_df['fecha_hora'].dt.year
    causa_df['mes'] = causa_df['fecha_hora'].dt.month
    causa_df['dia'] = causa_df['fecha_hora'].dt.day
    causa_df['hora'] = causa_df['fecha_hora'].dt.time
    causa_df = causa_df.set_index('id_choque')
    causa_df.drop('fecha_hora', axis=1, inplace=True)
    causa_df.drop('hora', axis=1, inplace=True)
    return causa_df.to_csv("/home/kevin/airflow/dataset_dump/causa_sql.csv")


def merge_master_data():
    df_causa = pd.read_csv("/home/kevin/airflow/dataset_dump/causa_sql.csv") 
    df_edad = pd.read_csv("/home/kevin/airflow/dataset_dump/edad_sql.csv")
    df_principal = pd.read_csv("/home/kevin/airflow/dataset_dump/principal_sql.csv")
    df_robo = pd.read_csv("/home/kevin/airflow/dataset_dump/robo_sql.csv") #listo
    frecuencia_robo =df_robo.groupby(['localidad']).count()
    frecuencia_robo.index
    robo= pd.DataFrame({'Localidad': frecuencia_robo.index, 'Cantidad_robos': frecuencia_robo['delito']})
    robo.reset_index(inplace=True)
    robo.drop('localidad',axis=1, inplace=True)
    df_causa = df_causa.drop_duplicates(
        subset=['id_choque'], keep=False)
    df_edad = df_edad.drop_duplicates(
        subset=['id_choque'], keep =False)
        
    merge_causa_ppal= pd.merge(df_principal,
                    df_causa[['id_choque','sexo', 'anio_del_vehiculo']],
                    on='id_choque', 
                    how='left')
    merge_causa_ppal.drop_duplicates()
    merge_causa_ppal= pd.merge(merge_causa_ppal,
                    df_edad[['id_choque','edad','rango_etario']],
                    on='id_choque', 
                    how='left')
    merge_causa_ppal.reset_index()
    Id_Fecha = merge_causa_ppal[['anio', 'mes', 'dia']]
    Id_Fecha = Id_Fecha[['anio', 'mes', 'dia']].drop_duplicates()
    Id_Fecha.reset_index(inplace=True)
    Id_Fecha['Id_Fecha'] = Id_Fecha.index
    merge_causa_ppal['anio']=merge_causa_ppal['anio'].astype(str)
    merge_causa_ppal['mes']=merge_causa_ppal['mes'].astype(str)
    merge_causa_ppal['dia']=merge_causa_ppal['dia'].astype(str)
    merge_causa_ppal['Fecha'] = (merge_causa_ppal['anio']) + (merge_causa_ppal['mes']) + (merge_causa_ppal['dia'])
    Id_Fecha['anio']=(Id_Fecha['anio']).astype(str)
    Id_Fecha['mes']=(Id_Fecha['mes']).astype(str) 
    Id_Fecha['dia']=(Id_Fecha['dia']).astype(str)
    Id_Fecha['Fecha'] = (Id_Fecha['anio']) + (Id_Fecha['mes']) + (Id_Fecha['dia'])
    merge_causa_ppal= pd.merge(merge_causa_ppal,
                    Id_Fecha[['Fecha', 'Id_Fecha']],
                    on='Fecha', 
                    how='left')
    merge_causa_ppal.drop(['anio','mes','dia','Fecha'], axis=1, inplace=True)
    Id_Fecha.set_index('Id_Fecha',inplace=True)
    merge_causa_ppal= merge_causa_ppal.drop_duplicates(
        subset=['id_choque'], keep=False)
    Id_Fecha.drop(['index', 'Fecha'], axis =1, inplace=True)
    Id_Ubicacion = merge_causa_ppal[['latitud','longitud','localidad','codigo_potal']]
    Id_Ubicacion =Id_Ubicacion.drop_duplicates()
    Id_Ubicacion.reset_index(inplace = True)
    Id_Ubicacion.drop('index', axis=1, inplace=True)
    Id_Ubicacion['Id_Ubicacion'] = Id_Ubicacion.index
    id_localidad = pd.DataFrame({'Id_Localidad': [1,2,3,4,5,6], 'localidad': Id_Ubicacion['localidad'].unique()})
    Id_Ubicacion= pd.merge(Id_Ubicacion,
                    id_localidad[['Id_Localidad', 'localidad']],
                    on='localidad', 
                    how='left')
    merge_causa_ppal= pd.merge(merge_causa_ppal,
                    Id_Ubicacion[['latitud', 'Id_Ubicacion', 'longitud','codigo_potal']],
                    on=['latitud','longitud','codigo_potal'])
    merge_causa_ppal.drop(['latitud', 'longitud', 'localidad','codigo_potal'], axis=1, inplace=True)
    Id_Ubicacion.set_index('Id_Ubicacion',inplace=True)
    id_localidad.set_index('Id_Localidad',inplace=True)
    Id_Ubicacion.drop('localidad',axis=1,inplace=True)
    id_tipo_auto = merge_causa_ppal['auto_causante']
    x = list(range(1,17))
    id_tipo_auto = pd.DataFrame({'Id_Tipo_Auto':x, 'auto_causante': id_tipo_auto.unique()})
    merge_causa_ppal= pd.merge(merge_causa_ppal,
                    id_tipo_auto[['Id_Tipo_Auto',	'auto_causante']],
                    on=['auto_causante'])
    merge_causa_ppal.drop(['auto_causante'], axis=1, inplace=True)
    id_tipo_auto.set_index('Id_Tipo_Auto', inplace=True)
    from cmath import nan
    Id_Rango_Etario = ['menor','adolescente','adulto_joven','adulto','adulto_mayor',nan]
    rangos = ['7-15','16-21','22-35','36-50','51 O mas', 'sin dato']
    x= list((range(1,7)))
    Id_Rango_Etario = pd.DataFrame({'Id_Rango_Etario':x, 'Edades':rangos, 'rango_etario':Id_Rango_Etario})
    merge_causa_ppal= pd.merge(merge_causa_ppal,
                    Id_Rango_Etario[['Id_Rango_Etario','rango_etario']],
                    on=['rango_etario'],
                    how ='left')
    merge_causa_ppal.drop(['rango_etario','edad'], axis=1, inplace=True)
    Id_Rango_Etario.set_index('Id_Rango_Etario', inplace= True)
    merge_causa_ppal.drop(['sexo'],axis=1,inplace=True)
    merge_causa_ppal['anio_del_vehiculo'].fillna(2019,inplace=True)
    merge_causa_ppal=merge_causa_ppal[ merge_causa_ppal['anio_del_vehiculo'] < 2023]
    id_letalidad =list( merge_causa_ppal['letalidad'].unique())
    id_letalidad.sort()
    x = list(range(len(id_letalidad)))
    #len(['Lesionados','Lesionados',z])
    id_letalidad = pd.DataFrame({'Id_Letalidad':x,'letalidad':id_letalidad,'Status_involucrados':['Lesionados','Lesionados','Muertes' ,'Muertes' ,'Muertes' ,'Muertes' ,'Muertes' ,'Muertes' ,'Muertes' ,'Muertes' ,'Muertes' ,'Muertes' ,'Muertes' ,'Muertes' ,'Muertes' ,'Muertes' ,'Muertes' ,'Muertes' ,'Muertes' ,'Muertes' ,'Muertes' ,'Muertes' ,'Muertes' ,'Muertes' ,'Muertes']})
    merge_causa_ppal= pd.merge(merge_causa_ppal,
                    id_letalidad[['Id_Letalidad','letalidad']],
                    on=['letalidad'],
                    how ='left')
    merge_causa_ppal.drop(['letalidad'], axis=1, inplace=True)
    id_letalidad.set_index('Id_Letalidad',inplace=True)
    id_modelo = (list(merge_causa_ppal['anio_del_vehiculo'].unique()))
    id_modelo.sort()
    x = range(1,(len(id_modelo)+1))
    Id_Anio_Vehiculo = pd.DataFrame({'Id_Anio_Vehiculo': x,'anio_del_vehiculo':id_modelo})
    merge_causa_ppal= pd.merge(merge_causa_ppal,
                    Id_Anio_Vehiculo[['Id_Anio_Vehiculo','anio_del_vehiculo']],
                    on=['anio_del_vehiculo'],
                    how ='left')
    merge_causa_ppal.drop(['anio_del_vehiculo'], axis=1, inplace=True)
    merge_causa_ppal.set_index('id_choque',inplace=True)
    merge_causa_ppal2= merge_causa_ppal[merge_causa_ppal['Id_Fecha']==1326]
    merge_causa_ppal2.to_csv('Accidentes_Master.csv')


def upload_master_to_s3():
    s3_hook= S3Hook(aws_conn_id= "AWS_S3_conn")
    s3_hook.load_file(
        filename= "/home/kevin/airflow/dataset_dump/Accidentes_Master.csv",
        key= "Accidentes_Master.csv",
        bucket_name= "proyecto-henry",
        replace= True)


with DAG(
    dag_id= 'Accidentes_ETL_Final',
    default_args= default_args,
    start_date= datetime(2022, 8, 30),
    schedule_interval= '@Monthly',
) as dag:


    task1= PythonOperator(
        task_id= "extracting_choques_nyc_from_api",
        python_callable= extract_choques_nyc_from_api
    )


    task2= PythonOperator(
        task_id= "extracting_robo_from_api",
        python_callable= extract_robo_from_api
    )


    task3= PythonOperator(
        task_id= "extracting_edad_from_api",
        python_callable= extract_edad_from_api
    )


    task4= PythonOperator(
        task_id= "extracting_edad_2_from_api",
        python_callable= extract_edad_2_from_api
    )


    task5= PythonOperator(
        task_id= "transforming_choques_nyc_from_api_extraction",
        python_callable= transform_main
    )

    task6= PythonOperator(
        task_id= "transforming_robo_from_api_extraction",
        python_callable= transform_robo
    )


    task7= PythonOperator(
        task_id= "transforming_edad_from_api_extration",
        python_callable= transform_edad
    )

    task8= PythonOperator(
        task_id= "transforming_edad_2_from_api_extraction",
        python_callable= transform_edad_2
    )

    task9= PythonOperator(
        task_id= "transforming_cause_from_api_extraction",
        python_callable= transform_causa
    )


    task10= PythonOperator(
        task_id= "merging_everything_into_master",
        python_callable= merge_master_data
    )

    task11= PythonOperator(
        task_id= "uploading_master_to_s3",
        python_callable= upload_master_to_s3
    )


    task1 >> task2 >> task3 >> task4 >> task5 >> task6 >> task7 >> task8 >> task9 >> task10 >> task11