# -*- coding: utf-8 -*-
import streamlit as st
import requests
import pandas as pd
import plotly.express as px

API_URL = "http://127.0.0.1:8000"

st.set_page_config(page_title="Spotify Data Analytics", layout="wide")
st.title("Spotify Data Analytics")
st.markdown("Analyse des features audio qui maximisent la popularite selon les decennies")

@st.cache_data
def get_token():
    r = requests.post(f"{API_URL}/token", data={"username": "admin", "password": "secret"})
    return r.json()["access_token"]

token = get_token()
headers = {"Authorization": f"Bearer {token}"}

@st.cache_data
def fetch(endpoint, size=100):
    r = requests.get(f"{API_URL}/{endpoint}?page=1&size={size}", headers=headers)
    return pd.DataFrame(r.json()["data"])

df_decade  = fetch("audio-by-decade")
df_tracks  = fetch("top-tracks", size=100)
df_genre   = fetch("genre-popularity", size=50)
df_artists = fetch("top-artists", size=50)

#Graphique 1 évolution features par decennie
st.subheader("Evolution des features audio par decennie (normalisees)")

df_decade_norm = df_decade.copy()
df_decade_norm["avg_popularity"] = df_decade_norm["avg_popularity"] / 100

fig1 = px.line(
    df_decade_norm,
    x="decade",
    y=["avg_popularity", "avg_danceability", "avg_energy"],
    markers=True,
    labels={"value": "Score (0-1)", "decade": "Decennie", "variable": "Feature"},
    color_discrete_map={
        "avg_popularity":   "#1DB954",
        "avg_danceability": "#FF6B6B",
        "avg_energy":       "#4ECDC4"
    }
)
st.plotly_chart(fig1, use_container_width=True)
#Graphique 2 top genres par popularite
st.subheader("Top 20 genres par popularite moyenne")
df_genre_top = df_genre.head(20)
fig2 = px.bar(
    df_genre_top,
    x="avg_popularity",
    y="genre",
    orientation="h",
    color="avg_danceability",
    color_continuous_scale="Viridis",
    labels={"avg_popularity": "Popularite moyenne", "genre": "Genre", "avg_danceability": "Danceability"}
)
fig2.update_layout(yaxis=dict(autorange="reversed"))
st.plotly_chart(fig2, use_container_width=True)

#graphique 3 danceability vs Energy (top tracks)
st.subheader("Danceability vs Energy des top tracks")
fig3 = px.scatter(
    df_tracks,
    x="danceability",
    y="energy",
    size="popularity",
    color="decade",
    hover_name="name",
    hover_data=["artist_name", "popularity"],
    labels={"danceability": "Danceability", "energy": "Energy", "decade": "Decennie"}
)
st.plotly_chart(fig3, use_container_width=True)

#Tableau top artistes par popularite
st.subheader("Top artistes")
st.dataframe(
    df_artists[["artist_name", "artist_popularity", "artist_followers", "nb_tracks", "avg_track_popularity"]]
    .head(20)
    .reset_index(drop=True),
    use_container_width=True
)