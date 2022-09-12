import streamlit as st
import base64
from PIL import Image

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
import numpy as np
import os
import PIL
import PIL.Image
import tensorflow as tf
import pathlib
data_dir = pathlib.Path(r'C:\Users\Alfredo BTP\Henry Data Science\Grupal\dataset drive\Riesgo-poliza-seguro\streamlit\Modelo\Detectar autos')
image_count = len(list(data_dir.glob('*/*.jpg')))
batch_size = 32
img_height = 180
img_width = 180
train_ds = tf.keras.utils.image_dataset_from_directory(
  data_dir,
  validation_split=0.2,
  subset="training",
  seed=123,
  image_size=(img_height, img_width),
  batch_size=batch_size)
val_ds = tf.keras.utils.image_dataset_from_directory(
  data_dir,
  validation_split=0.2,
  subset="validation",
  seed=123,
  image_size=(img_height, img_width),
  batch_size=batch_size)
class_names = train_ds.class_names
AUTOTUNE = tf.data.AUTOTUNE

train_ds = train_ds.cache().prefetch(buffer_size=AUTOTUNE)
val_ds = val_ds.cache().prefetch(buffer_size=AUTOTUNE)
num_classes = 5

model = tf.keras.Sequential([
  tf.keras.layers.Rescaling(1./255),
  tf.keras.layers.Conv2D(32, 3, activation='relu'),
  tf.keras.layers.MaxPooling2D(),
  tf.keras.layers.Conv2D(32, 3, activation='relu'),
  tf.keras.layers.MaxPooling2D(),
  tf.keras.layers.Conv2D(32, 3, activation='relu'),
  tf.keras.layers.MaxPooling2D(),
  tf.keras.layers.Flatten(),
  tf.keras.layers.Dense(128, activation='relu'),
  tf.keras.layers.Dense(num_classes)
])
model.compile(
  optimizer='adam',
  loss=tf.losses.SparseCategoricalCrossentropy(from_logits=True),
  metrics=['accuracy'])
model.fit(
  train_ds,
  validation_data=val_ds,
  epochs=6
)
list_ds = tf.data.Dataset.list_files(str(data_dir/'*/*'), shuffle=False)
list_ds = list_ds.shuffle(image_count, reshuffle_each_iteration=False)
class_names = np.array(sorted([item.name for item in data_dir.glob('*') if item.name != "LICENSE.txt"]))
val_size = int(image_count * 0.2)
train_ds = list_ds.skip(val_size)
val_ds = list_ds.take(val_size)
def get_label(file_path):
  # Convert the path to a list of path components
  parts = tf.strings.split(file_path, os.path.sep)
  # The second to last is the class-directory
  one_hot = parts[-2] == class_names
  # Integer encode the label
  return tf.argmax(one_hot)
def decode_img(img):
  # Convert the compressed string to a 3D uint8 tensor
  img = tf.io.decode_jpeg(img, channels=3)
  # Resize the image to the desired size
  return tf.image.resize(img, [img_height, img_width])
def process_path(file_path):
  label = get_label(file_path)
  # Load the raw data from the file as a string
  img = tf.io.read_file(file_path)
  img = decode_img(img)
  return img, label
# Set `num_parallel_calls` so multiple images are loaded/processed in parallel.
train_ds = train_ds.map(process_path, num_parallel_calls=AUTOTUNE)
val_ds = val_ds.map(process_path, num_parallel_calls=AUTOTUNE)
for image, label in train_ds.take(1):
  print("Image shape: ", image.numpy().shape)
  print("Label: ", label.numpy())
def configure_for_performance(ds):
  ds = ds.cache()
  ds = ds.shuffle(buffer_size=1000)
  ds = ds.batch(batch_size)
  ds = ds.prefetch(buffer_size=AUTOTUNE)
  return ds

train_ds = configure_for_performance(train_ds)
val_ds = configure_for_performance(val_ds)
image_batch, label_batch = next(iter(train_ds))

model.fit(
  train_ds,
  validation_data=val_ds,
  epochs=5
)



tit1= '<p style="font-family:sans-serif; color:#124183; text-align:center; font-size: 20px;"><b>¿No conoces tu tipo de auto?</b></p>'
st.markdown(tit1, unsafe_allow_html=True)
tit1= '<p style="font-family:sans-serif; color:#124183; text-align:center; font-size: 18px;"><b>Nosotros te ayudamos</b></p>'
st.markdown(tit1, unsafe_allow_html=True)
uploaded_files = st.file_uploader("Cargar archivo")

if uploaded_files is None:
    tit1= '<p style="font-family:sans-serif; color:#124183; text-align:center; font-size: 18px;"><b>Introduce una foto de tu auto para conocer su tipo</b></p>'
    st.markdown(tit1, unsafe_allow_html=True)
elif uploaded_files is not None:
    foto = uploaded_files

    img = tf.keras.utils.load_img(
    foto, target_size=(img_height, img_width)
    )
    img_array = tf.keras.utils.img_to_array(img)
    img_array = tf.expand_dims(img_array, 0) # Create a batch

    predictions = model.predict(img_array)
    score = tf.nn.softmax(predictions[0])
    
    st.image(uploaded_files)
    texto= (
    "El tipo de auto es: {} "
    .format(class_names[np.argmax(score)], 100 * np.max(score)))
    tit1= f'<p style="font-family:sans-serif; color:#124183; background-color:#C1E5F0; text-align:center; font-size: 30px;"><b>{texto}</b></p>'
    st.markdown(tit1, unsafe_allow_html=True)
