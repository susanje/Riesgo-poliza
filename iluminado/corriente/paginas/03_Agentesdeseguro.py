import plotly.graph_objects as go
import streamlit as st
import pandas as pd
from PIL import Image
import base64
image3 = Image.open(r"C:\Users\Alfredo BTP\OneDrive\Documentos\riesgos\logokeva-removebg-preview.png")
st.set_page_config(page_title='KEVA-seguros', page_icon=image3)
@st.experimental_memo
def get_img_as_base64(file):
    with open(file, "rb") as f:
        data = f.read()
    return base64.b64encode(data).decode()


img = get_img_as_base64(r"C:\Users\Alfredo BTP\OneDrive\Documentos\riesgos\lateral22.jpg")
page_bg_img = f"""
<style>
[data-testid="stAppViewContainer"] > .main {{
background-image: url("https://media.istockphoto.com/illustrations/minimal-geometric-blue-light-background-abstract-design-illustration-id1193878242?k=20&m=1193878242&s=612x612&w=0&h=ThcVvL--w394lYgqraiFPwL_5PZ8b1Q63RJPPSiPsCM=");
background-size: cover;
}}
[data-testid="stSidebar"] > div:first-child {{
background-image: url("data:image/png;base64,{img}");
background-size: cover;
}}
[data-testid="stHeader"] {{
background: rgba(0,0,0,0);
}}
[data-testid="stToolbar"] {{
right: 2rem;
}}
</style>
"""
st.markdown(page_bg_img,unsafe_allow_html=True)
#Abre csv
df = pd.read_csv(r"C:\Users\Alfredo BTP\Henry Data Science\Grupal\dataset drive\Riesgo-poliza-seguro\datos_stream.csv")
var = 0
image = Image.open(r"C:\Users\Alfredo BTP\OneDrive\Documentos\riesgos\porta.png")
image1 = Image.open(r"C:\Users\Alfredo BTP\OneDrive\Documentos\riesgos\segu211.png")
image2 = Image.open(r"C:\Users\Alfredo BTP\OneDrive\Documentos\riesgos\eva1.png")
st.image(image)



usuario = st.text_input('Introduce tu nombre de usuario')
pasword = st.text_input('Introduce tu contraseña',type="password")
if (usuario == 'Pablo' or usuario == 'Paula') and pasword == 'admin':
        if usuario =='Pablo':
            tit13= '<p style="font-family:sans-serif; color:#173983; text-align:center; font-size: 20px;"><b>Bienvenido Pablo</b></p>'
            st.markdown(tit13, unsafe_allow_html=True)
        elif usuario == 'Paula':
            tit12= '<p style="font-family:sans-serif; color:#B55EE3; text-align:center; font-size: 20px;"><b>Bienvenida Paula</b></p>'
            st.markdown(tit12, unsafe_allow_html=True)
        tit10= '<p style="font-family:sans-serif; color:#124183; text-align:center; font-size: 20px;"><b>Completa el formulario para obtener el reisgo de siniestro y su costo</b></p>'
        st.markdown(tit10, unsafe_allow_html=True)
        #************************CREACION DE SELECT BOX*****************************************
        #Localidad de vivienda
        #Formar lista de localidades
        localidades = list(df.localidad.unique())
        localidades.pop(2)
        localidades.insert(0," ")
        tit4= '<p style="font-family:sans-serif; color:#210488; font-size: 16px;">Selecciona  la localidad donde vives</p>'
        st.markdown(tit4, unsafe_allow_html=True)
        localidad_v =st.selectbox('Elige una opción',localidades,0)
        #Localidad ded manejo
        tit5= '<p style="font-family:sans-serif; color:#210488; font-size: 16px;">Selecciona la localidad donde mas ciruclas (puede ser la misma)</p>'
        st.markdown(tit5, unsafe_allow_html=True)
        localidad_m=st.selectbox('Elige una opción ', localidades,0)
        #Auto que conduce
        #Lista de autos
        autos = list(df.auto_causante.unique())
        autos.sort()
        autos.pop(3)
        autos.insert(0, " ")
        tit6= '<p style="font-family:sans-serif; color:#210488; font-size: 16px;">Selecciona el tipo de auto que conduces</p>'
        st.markdown(tit6, unsafe_allow_html=True)
        auto = st.selectbox('Elige una opción', autos,0)
        #Modelo del auto
        modelo = [" ", 2022,2021,2020,2019,2018,2017,2016,2015,2014,2013,2012]
        tit7= '<p style="font-family:sans-serif; color:#210488; font-size: 16px;">Selecciona año del modelo del automovil</p>'
        st.markdown(tit7, unsafe_allow_html=True)
        auto_modelo = st.selectbox('Elige una opción', modelo,0)
        #Rango etario
        #lista rangos
        rango_etario = list(df.rango_etario.unique())
        rango_etario.pop(0)
        rango_etario.sort()
        rango_etario.insert(0, " ")
        tit8= '<p style="font-family:sans-serif; color:#210488; font-size: 16px;">Selecciona la edad del conductor principal</p>'
        st.markdown(tit8, unsafe_allow_html=True)
        rango= st.selectbox('Elige una opción',rango_etario,0)

        #**********************************ASIGNACION DE % DE CHOQUE Y RIESGO***************************
        #Asignacion de % por la probabildiad de choque da cada vairable
        porcentajes_choque = pd.DataFrame({'Concepto':['Robo_localidad','Choques localidad vivienda','Choques localidad manejo',
                                    'Choques edad vivienda','Choques edad manejo','Choques tipo auto vivienda', 
                                    'Choques tipo auto manejo'],'Porcentaje':[12.5,25,17.5,12.5,12.5,10,10]})
        #Costos base poliza
        #costos = pd.DataFrame({'Tipo vehiculo':autos,'Precio_base':[400,300,450,]})
                                    
        #*********************Calculo de probabilidad de choque **********************************
        #data filtrada por localidad donde maneja mas
        #data filtrada por localidad de vivienda
        if localidad_v == " " or localidad_m == " " or auto == " " or auto_modelo == " " or rango == " ":
            tit9= '<p style="font-family:sans-serif; color:#124183; text-align:center; font-size: 29px;"><b>Calculando...</b></p>'
            st.markdown(tit9, unsafe_allow_html=True)
        else:
            suma_probabilidad = 0
            df_vivienda = df[df['localidad']==localidad_v]
            df_manejo = df[df['localidad']==localidad_m]
            choques_por_localidad = df.localidad.value_counts().sum()
            choques_por_localidad_v = df_vivienda.id_choque.count()
            choques_por_localidad_m = df_manejo.id_choque.count()
            #Porcentaje de choques por localidad elegida
            v1 = 100/choques_por_localidad * choques_por_localidad_v
            m1 =  100/choques_por_localidad * choques_por_localidad_m
            #Transformar al % asignado
            suma_probabilidad += (v1*(porcentajes_choque['Porcentaje'][1])/100)
            suma_probabilidad +=(m1/(100/porcentajes_choque['Porcentaje'][2]))
            #Accidentes y % por tipo de auto conducido según su localidad
            auto_viv = df[(df.localidad == localidad_v)&(df.auto_causante == auto)]
            auto_viv_tot = df_vivienda.auto_causante.value_counts().sum()
            auto_man = df[(df.localidad == localidad_m)&(df.auto_causante == auto)]
            auto_man_tot = df_manejo.auto_causante.value_counts().sum()
            choques_auto_viv = auto_viv.id_choque.count()
            choques_auto_man = auto_man.id_choque.count()
            v2= 100/auto_viv_tot*choques_auto_viv
            m2= 100/ auto_man_tot*choques_auto_man
            suma_probabilidad += v2*(porcentajes_choque['Porcentaje'][5]/100)
            suma_probabilidad+= m2*(porcentajes_choque['Porcentaje'][6]/100)

            #Cantidad de accidentes por rango etario y % en su respectiva localidad
            etario_viv = df[(df.localidad == localidad_v)&(df.rango_etario ==rango)]
            etario_viv_tot = df_vivienda.rango_etario.value_counts().sum()
            etario_man = df[(df.localidad == localidad_m)&(df.rango_etario ==rango)]
            etario_man_tot = df_manejo.rango_etario.value_counts().sum()
            choques_etario_viv = etario_viv.id_choque.count()
            choques_etario_man = etario_man.id_choque.count()
            v= 100/etario_viv_tot*choques_etario_viv
            m= 100/ etario_man_tot*choques_etario_man
            suma_probabilidad += v*(porcentajes_choque['Porcentaje'][3]/100)
            suma_probabilidad+= m*(porcentajes_choque['Porcentaje'][4]/100)
            #Modelo de auto
            if auto_modelo >2020:
                suma_probabilidad += 2
            elif auto_modelo <= 2020 and auto_modelo >2017:
                suma_probabilidad += 4
            elif auto_modelo <=2017:
                suma_probabilidad += 2

            if suma_probabilidad > 35:
                prob = 'Muy alto'
            elif suma_probabilidad >= 30  and suma_probabilidad <35:
                prob = 'Alto'
            elif suma_probabilidad >= 20  and suma_probabilidad <30:
                prob = 'Medio'
            elif suma_probabilidad >= 10  and suma_probabilidad <20:
                prob = 'Bajo'
            elif suma_probabilidad >=0  and suma_probabilidad <10:
                prob = 'Muy bajo'
            df_fatalidad = df[(df.localidad == localidad_v)&(df.rango_etario ==rango)&(df.auto_causante == auto)]
            fatal = df_fatalidad['letalidad'].mean()
            autos_involucrados = df_fatalidad['cantidad_autos_choque'].mean()
            if fatal > 1.3:
                f = 4
            elif fatal <=1.3 and fatal > 1:
                f = 3
            elif fatal <=1 and fatal > 0.75:
                f = 2
            elif fatal <= 0.75 and fatal > 0.4:
                f = 1
            else:
                f = 0
            if autos_involucrados > 3:
                a = 4
            elif autos_involucrados <= 3 and autos_involucrados> 2.5:
                a = 3
            elif autos_involucrados <=2.5 and autos_involucrados >2.1:
                a = 2
            elif autos_involucrados <= 2.1 and autos_involucrados > 1.3:
                a= 1
            else:
                a = 0
            costo_accidente = f + a
            if costo_accidente >= 7:
                riesgo = 'Muy alto'
            elif costo_accidente <7 and costo_accidente >=5:
                riesgo = 'Alto'
            elif costo_accidente <5 and costo_accidente >=3:
                riesgo = 'Regular' 
            elif costo_accidente <3 and costo_accidente >=1:
                riesgo = 'Bajo' 
            else:
                riesgo = 'Muy bajo'

            #st.text('Probabilidad de verse involucrado en un siniestro (robo o choque)')
            #st.markdown(f'{suma_probabilidad}% // su probabilidad es {prob} y el riesgo de verse involucrado en un accidente muy costoso es {riesgo}')
            col1, col2= st.columns(2)
            with col1:
                fig = go.Figure(go.Indicator(
                mode = "gauge+number+delta",
                value = suma_probabilidad,
                domain = {'x': [0, 1], 'y': [0, 1]},
                title = {'text': ("El riesgo de siniestro es "+prob), 'font': {'size': 24}},
                #delta = {'reference': 32, 'increasing': {'color': "#0B366F"}},
                gauge = {
                    'axis': {'range': [5, 42], 'tickwidth': 1, 'tickcolor': "white"},
                    'bar': {'color': "black"},
                    'bgcolor': "orange",
                    'borderwidth': 3,
                    'bordercolor': "#676A6D",
                    'steps': [
                        {'range': [5, 10], 'color': '#2CA80B'},
                        {'range': [10.01, 20], 'color': '#C5E20E'},
                        {'range': [20.01, 30], 'color': '#E2C50E'},
                        {'range': [30.01,34.99], 'color': '#E2580E'},
                        {'range': [35, 42], 'color': '#E2150E'}],
                    'threshold': {
                        'line': {'color': "red", 'width': 4},
                        'thickness': 0.75,
                        'value': 41.5}}))
                fig.update_layout(paper_bgcolor = "#DDDDDE", font = {'color': "darkblue", 'family': "Arial"})
                fig.update_layout(width=350, height=250)
                st.plotly_chart(fig)
            with col2:
                fig2 = go.Figure(go.Indicator(
                mode = "gauge+number+delta",
                value = costo_accidente,
                domain = {'x': [0, 1], 'y': [0, 1]},
                title = {'text': ("El costo el accidente es "+riesgo), 'font': {'size': 24}},
                #delta = {'reference': 32, 'increasing': {'color': "#0B366F"}},
                gauge = {
                    'axis': {'range': [0, 9], 'tickwidth': 1, 'tickcolor': "black"},
                    'bar': {'color': "black"},
                    'bgcolor': "orange",
                    'borderwidth': 3,
                    'bordercolor': "#676A6D",
                    'steps': [
                        {'range': [0, 1], 'color': '#2CA80B'},
                        {'range': [1.01, 2.99], 'color': '#C5E20E'},
                        {'range': [3, 4.99], 'color': '#E2C50E'},
                        {'range': [5,6.99], 'color': '#E2580E'},
                        {'range': [7, 9], 'color': '#E2150E'}],
                    'threshold': {
                        'line': {'color': "red", 'width': 4},
                        'thickness': 0.75,
                        'value': 8.8}}))

                fig2.update_layout(paper_bgcolor = "#DDDDDE", font = {'color': "darkblue", 'family': "Arial"})
                fig2.update_layout(width=350, height=250)
                st.plotly_chart(fig2)
elif usuario == "" or pasword == "":
        tit9= '<p style="font-family:sans-serif; color:#124183; text-align:center; font-size: 29px;"><b>Si olvidaste tus datos contacta a un administrador</b></p>'
        st.markdown(tit9, unsafe_allow_html=True)
else:
        tit14= '<p style="font-family:sans-serif; color:#EB1212; text-align:center; font-size: 29px;"><b>Error favor de intentar nuevamente</b></p>'
        st.markdown(tit14, unsafe_allow_html=True)
#else:
   # tit9= '<p style="font-family:sans-serif; color:#124183; text-align:center; font-size: 29px;"><b>Elige la opcion deseada</b></p>'
  #  st.markdown(tit9, unsafe_allow_html=True)
