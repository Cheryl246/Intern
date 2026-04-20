from datetime import date

import matplotlib.pyplot as plt
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import pyodbc
import streamlit as st
from matplotlib.ticker import FuncFormatter


st.set_page_config(page_title="Dashboard Produksi", layout="wide")


DEFAULT_START_DATE = date(2023, 1, 1)
DEFAULT_END_DATE = date(2025, 12, 31)

SECTION_OPTIONS = {
    "Ringkasan & View Panen": "ringkasan_view",
    "Produksi": "produksi",
    "Panen vs Produksi": "panen_vs_produksi",
    "BJR Panen": "bjr_panen",
    "Panen per Blok": "panen_blok",
    "Produktivitas Perawatan": "produktif_perawatan",
}

MONTH_MAP_ID = {
    1: "Januari",
    2: "Februari",
    3: "Maret",
    4: "April",
    5: "Mei",
    6: "Juni",
    7: "Juli",
    8: "Agustus",
    9: "September",
    10: "Oktober",
    11: "November",
    12: "Desember",
}



@st.cache_resource
def get_connection():
    conn_str = (
        "DRIVER={SQL Server};"
        "SERVER=10.131.1.122;"
        "DATABASE=CUSG;"
        "UID=ai5;"
        "PWD=pas123456;"
        "MARS_Connection=Yes;"
    )
    return pyodbc.connect(conn_str)


@st.cache_data(show_spinner=False, ttl=900)
def run_query(query: str) :
    return pd.read_sql(query, get_connection())


def format_angka(value):
    return "{:,.0f}".format(value).replace(",", ".")


def format_angka_desimal(value):
    return "{:,.2f}".format(value).replace(",", "_").replace(".", ",").replace("_", ".")


def ribuan_formatter(value, _pos):
    return format_angka(value)


def format_bulan_indonesia(series: pd.Series) -> pd.Series:
    return series.dt.month.map(MONTH_MAP_ID) + " " + series.dt.year.astype(str)


def normalize_text(value):
    if pd.isna(value):
        return ""
    return str(value).strip().upper().replace(" ", "").replace("/", "").replace("-", "")


def normalize_series(series: pd.Series) -> pd.Series:
    return (
        series.fillna("")
        .astype(str)
        .str.strip()
        .str.upper()
        .str.replace(" ", "", regex=False)
        .str.replace("/", "", regex=False)
        .str.replace("-", "", regex=False)
    )


def filter_dataframe_by_date(df_source, date_column, start_date, end_date):
    if df_source.empty or date_column not in df_source.columns:
        return df_source.copy()
    start_ts = pd.Timestamp(start_date)
    end_ts = pd.Timestamp(end_date)
    date_series = pd.to_datetime(df_source[date_column], errors="coerce").dt.normalize()
    return df_source.loc[date_series.between(start_ts, end_ts)].copy()


def resolve_date_range(date_value):
    if isinstance(date_value, tuple) and len(date_value) == 2:
        return date_value[0], date_value[1]
    if isinstance(date_value, list) and len(date_value) == 2:
        return date_value[0], date_value[1]
    return DEFAULT_START_DATE, DEFAULT_END_DATE


@st.cache_data(show_spinner=False, ttl=900)
def load_panen_data(ttl=900):
    query = """
    SELECT
        Tanggal,
        [Berat Netto],
        Estate,
        Divisi,
        Blok
    FROM dbo.PMS_Premi_PanenInquiry_Anggota3
    WHERE Tanggal >= '2023-01-01'
      AND Tanggal < '2026-01-01'
    """
    df = run_query(query)
    df.columns = df.columns.str.strip().str.upper()
    df["TANGGAL"] = pd.to_datetime(df["TANGGAL"], errors="coerce")
    df = df.dropna(subset=["TANGGAL"]).copy()
    df["BERAT NETTO"] = pd.to_numeric(df["BERAT NETTO"], errors="coerce").fillna(0)
    for col in ["ESTATE", "DIVISI", "BLOK"]:
        df[col] = df[col].astype(str).str.strip().str.upper()
    return df


@st.cache_data(show_spinner=False, ttl=900)
def load_view_panen():
    query = """
    SELECT
        [TGL TRX] AS TANGGAL,
        [ESTATE ACTIVITY] AS ESTATE,
        [DIVISI ACTIVITY] AS DIVISI,
        [HASIL JJG]
    FROM dbo.VIEW_PANEN_TRX
    WHERE [TGL TRX] >= '2023-01-01'
      AND [TGL TRX] < '2026-01-01'
    """
    df = run_query(query)
    df.columns = df.columns.str.strip().str.upper()
    df["TANGGAL"] = pd.to_datetime(df["TANGGAL"], errors="coerce")
    df = df.dropna(subset=["TANGGAL"]).copy()
    df["HASIL JJG"] = pd.to_numeric(df["HASIL JJG"], errors="coerce").fillna(0)
    for col in ["ESTATE", "DIVISI"]:
        df[col] = df[col].astype(str).str.strip().str.upper()
    return df


@st.cache_data(show_spinner=False, ttl=900)
def load_produksi_data():
    query = """
    SELECT
        [Tanggal Muat],
        [Estate/Kode Vendor],
        [Divisi],
        [Netto BJR]
    FROM dbo.wb_timbang_tbs_by_detail
    WHERE [Tanggal Muat] >= '2023-01-01'
      AND [Tanggal Muat] < '2026-01-01'
    """
    df = run_query(query)
    df.columns = df.columns.str.strip()
    df["Tanggal Muat"] = pd.to_datetime(df["Tanggal Muat"], errors="coerce")
    df = df.dropna(subset=["Tanggal Muat"]).copy()
    df["Netto BJR"] = pd.to_numeric(df["Netto BJR"], errors="coerce").fillna(0)
    df["ESTATE_NORMALIZED"] = normalize_series(df["Estate/Kode Vendor"])
    df["DIVISI_NORMALIZED"] = normalize_series(df["Divisi"])
    return df


@st.cache_data(show_spinner=False, ttl=900)
def load_komidel_data():
    query = """
    SELECT
        [Tanggal Awal],
        Estate,
        Divisi,
        Blok,
        [BJR Panen]
    FROM dbo.PAS_Komidel
    WHERE [Tanggal Awal] >= '2023-01-01'
      AND [Tanggal Awal] < '2026-01-01'
    """
    df = run_query(query)
    df.columns = df.columns.str.strip().str.upper()
    df["TANGGAL AWAL"] = pd.to_datetime(df["TANGGAL AWAL"], errors="coerce")
    df["BJR PANEN"] = pd.to_numeric(df["BJR PANEN"], errors="coerce")
    df = df.dropna(subset=["TANGGAL AWAL", "BJR PANEN"]).copy()
    for col in ["ESTATE", "DIVISI", "BLOK"]:
        df[col] = df[col].astype(str).str.strip().str.upper()
    df["BULAN"] = df["TANGGAL AWAL"].dt.to_period("M").dt.to_timestamp()
    return df


@st.cache_data(show_spinner=False, ttl=900)
def load_blok_data():
    query = """
    SELECT
        Tanggal AS TANGGAL,
        Estate AS ESTATE_ACTIVITY,
        Divisi AS DIVISI_ACTIVITY,
        Blok AS BLOK_ACTIVITY,
        [Berat Netto] AS [NETTO BJR]
    FROM dbo.PMS_Premi_PanenInquiry_Anggota3
    WHERE Tanggal >= '2023-01-01'
      AND Tanggal < '2026-01-01'
    """
    df = run_query(query)
    df.columns = df.columns.str.strip()
    df["TANGGAL"] = pd.to_datetime(df["TANGGAL"], errors="coerce")
    df = df.dropna(subset=["TANGGAL"]).copy()
    df["NETTO BJR"] = pd.to_numeric(df["NETTO BJR"], errors="coerce").fillna(0)
    for col in ["ESTATE_ACTIVITY", "DIVISI_ACTIVITY", "BLOK_ACTIVITY"]:
        df[col] = df[col].astype(str).str.strip().str.upper()
    return df


@st.cache_data(show_spinner=False, ttl=900)
def load_area_data():
    query = """
    SELECT
        [PMS_Area_Label1ID] AS ESTATE_ACTIVITY,
        [PMS_Area_Label2ID] AS DIVISI_ACTIVITY,
        [PMS_Area_Label3ID] AS BLOK_ACTIVITY,
        [PMS_TotalArea] AS LUAS_HEKTAR
    FROM dbo.area_statement_smartlist
    WHERE PMS_Indicator_Status LIKE 'TM%'
    """
    df = run_query(query)
    df.columns = df.columns.str.strip()
    for col in ["ESTATE_ACTIVITY", "DIVISI_ACTIVITY", "BLOK_ACTIVITY"]:
        df[col] = df[col].astype(str).str.strip().str.upper()
    df["LUAS_HEKTAR"] = pd.to_numeric(df["LUAS_HEKTAR"], errors="coerce")
    return df


@st.cache_data(show_spinner=False, ttl=900)
def load_perawatan_data():
    query = """
    SELECT
        [TGL TRX],
        [ESTATE ACTIVITY],
        [DIVISI ACTIVITY],
        [BLOK ACTIVITY],
        [NAMA AKTIVITAS],
        [TOTAL ANGGOTA],
        [HEKTAR]
    FROM PMS_TRX_PERAWATAN
    WHERE YEAR([TGL TRX]) BETWEEN 2023 AND 2025
    """
    df = run_query(query)
    df.columns = df.columns.str.strip()
    df["TGL TRX"] = pd.to_datetime(df["TGL TRX"], errors="coerce")
    df["HEKTAR"] = pd.to_numeric(df["HEKTAR"], errors="coerce")
    df["TOTAL ANGGOTA"] = pd.to_numeric(df["TOTAL ANGGOTA"], errors="coerce")
    df = df.dropna(subset=["TGL TRX", "HEKTAR", "TOTAL ANGGOTA"]).copy()
    return df


def render_ringkasan_panen(start_date, end_date):
    df = filter_dataframe_by_date(load_panen_data(), "TANGGAL", start_date, end_date)

    st.subheader("Ringkasan Produksi 2023-2025")
    total_kg = df["BERAT NETTO"].sum()
    jumlah_hari = df["TANGGAL"].dt.date.nunique()
    rata_harian = total_kg / jumlah_hari if jumlah_hari > 0 else 0

    col1, col2, col3 = st.columns(3)
    col1.metric("Total HASIL KG", format_angka(total_kg))
    col2.metric("Rata-rata per Hari", format_angka(rata_harian))
    col3.metric("Jumlah Hari Panen", f"{format_angka(jumlah_hari)} Hari")

    estate_list = sorted(df["ESTATE"].dropna().unique())
    if not estate_list:
        st.warning("Tidak ada data panen pada range tanggal yang dipilih.")
        return

    selected_estate = st.selectbox("Pilih Estate", estate_list, key="estate_ringkasan")
    df_estate = df[df["ESTATE"] == selected_estate].copy()

    st.subheader(f"Tren Panen Bulanan Estate {selected_estate}")
    df_estate["BULAN"] = df_estate["TANGGAL"].dt.to_period("M").dt.to_timestamp()
    panen_bulanan = (
        df_estate.groupby(["BULAN", "DIVISI"])["BERAT NETTO"]
        .sum()
        .reset_index()
        .sort_values(["DIVISI", "BULAN"])
    )

    if panen_bulanan.empty:
        st.info("Belum ada data panen bulanan untuk estate yang dipilih.")
    else:
        fig, ax = plt.subplots(figsize=(10, 4))
        for div in sorted(panen_bulanan["DIVISI"].dropna().unique()):
            df_div = panen_bulanan[panen_bulanan["DIVISI"] == div].sort_values("BULAN")
            ax.plot(df_div["BULAN"], df_div["BERAT NETTO"], marker="o", linewidth=2, label=div)
        ax.grid(True, alpha=0.3)
        ax.set_xlabel("Bulan")
        ax.set_ylabel("Total KG")
        ax.yaxis.set_major_formatter(FuncFormatter(ribuan_formatter))
        ax.tick_params(axis="x", rotation=45)
        ax.legend(title="Divisi", bbox_to_anchor=(1.02, 1), loc="upper left")
        fig.tight_layout()
        st.pyplot(fig)

    st.subheader("Tabel Panen Bulanan")
    tabel = (
        df_estate.groupby(["DIVISI", "BULAN"])["BERAT NETTO"]
        .sum()
        .reset_index()
        .sort_values(["DIVISI", "BULAN"])
    )
    tabel["BULAN"] = format_bulan_indonesia(tabel["BULAN"])
    st.dataframe(tabel.style.format({"BERAT NETTO": format_angka}), use_container_width=True)


def render_view_panen(start_date, end_date):
    df = filter_dataframe_by_date(load_view_panen(), "TANGGAL", start_date, end_date)
    st.subheader("Tren Panen (Hasil JJG) dari VIEW_PANEN_TRX")

    estate_list = sorted(df["ESTATE"].dropna().unique())
    if not estate_list:
        st.info("Data VIEW_PANEN_TRX tidak tersedia pada range tanggal yang dipilih.")
        return

    selected_estate = st.selectbox("Pilih Estate", estate_list, key="estate_view_panen_chart")
    df_estate = df[df["ESTATE"] == selected_estate].copy()
    df_estate["BULAN"] = df_estate["TANGGAL"].dt.to_period("M").dt.to_timestamp()

    view_panen_bulanan = (
        df_estate.groupby(["BULAN", "DIVISI"])["HASIL JJG"]
        .sum()
        .reset_index()
        .sort_values("BULAN")
    )

    fig, ax = plt.subplots(figsize=(10, 4))
    for div in view_panen_bulanan["DIVISI"].dropna().unique():
        df_div = view_panen_bulanan[view_panen_bulanan["DIVISI"] == div]
        ax.plot(df_div["BULAN"], df_div["HASIL JJG"], marker="o", label=div)

    ax.set_title(f"Tren Panen Bulanan Estate {selected_estate} dari VIEW_PANEN_TRX")
    ax.set_xlabel("Bulan")
    ax.set_ylabel("Hasil JJG")
    ax.yaxis.set_major_formatter(FuncFormatter(ribuan_formatter))
    ax.legend()
    ax.grid(True, alpha=0.3)
    plt.xticks(rotation=45)
    st.pyplot(fig)

    tabel = view_panen_bulanan.copy()
    tabel["PERIODE"] = format_bulan_indonesia(tabel["BULAN"])
    st.subheader("Tabel Tren Panen (Hasil JJG)")
    st.dataframe(
        tabel[["PERIODE", "DIVISI", "HASIL JJG"]].style.format({"HASIL JJG": format_angka}),
        use_container_width=True,
    )


def render_produksi(start_date, end_date):
    df_prod = filter_dataframe_by_date(load_produksi_data(), "Tanggal Muat", start_date, end_date)

    required_cols = ["Estate/Kode Vendor", "Divisi", "Tanggal Muat", "Netto BJR"]
    if not all(col in df_prod.columns for col in required_cols):
        st.error("Kolom produksi tidak lengkap.")
        return

    st.subheader("Grafik Produksi Bulanan")

    estate_list = sorted(df_prod["Estate/Kode Vendor"].dropna().unique())
    if not estate_list:
        st.info("Data produksi tidak tersedia.")
        return

    selected_estate = st.selectbox("Pilih Estate (Produksi)", estate_list, key="estate_produksi")
    estate_key = normalize_text(selected_estate)

    df_estate = df_prod[df_prod["ESTATE_NORMALIZED"] == estate_key].copy()
    df_estate["BULAN"] = df_estate["Tanggal Muat"].dt.to_period("M").dt.to_timestamp()

    prod_bulanan_div = (
        df_estate.groupby(["BULAN", "Divisi"])["Netto BJR"]
        .sum()
        .reset_index()
        .sort_values("BULAN")
    )

    fig, ax = plt.subplots(figsize=(10, 4))
    for div in prod_bulanan_div["Divisi"].dropna().unique():
        df_div = prod_bulanan_div[prod_bulanan_div["Divisi"] == div]
        ax.plot(df_div["BULAN"], df_div["Netto BJR"], marker="o", label=div)

    ax.set_title(f"Tren produksi bulanan estate {selected_estate}")
    ax.set_xlabel("Bulan")
    ax.set_ylabel("Netto BJR (Kg)")
    ax.yaxis.set_major_formatter(FuncFormatter(ribuan_formatter))
    ax.legend()
    ax.grid(True, alpha=0.3)
    plt.xticks(rotation=45)

    st.pyplot(fig)

    # TABEL (WAJIB TETAP ADA)
    st.subheader("Tabel Produksi Bulanan")

    tabel = (
        df_estate.groupby(["Divisi", "BULAN"])["Netto BJR"]
        .sum()
        .reset_index()
        .sort_values(["Divisi", "BULAN"])
    )

    tabel["BULAN"] = format_bulan_indonesia(tabel["BULAN"])

    st.dataframe(
        tabel.style.format({"Netto BJR": format_angka}),
        use_container_width=True,
    )

def render_panen_vs_produksi(start_date, end_date):
    st.subheader("Grafik Panen vs Produksi")

    df_prod = filter_dataframe_by_date(load_produksi_data(), "Tanggal Muat", start_date, end_date)
    df_panen = filter_dataframe_by_date(load_panen_data(), "TANGGAL", start_date, end_date)

    df_panen["ESTATE_NORMALIZED"] = normalize_series(df_panen["ESTATE"])
    df_panen["DIVISI_NORMALIZED"] = normalize_series(df_panen["DIVISI"])

    estate_list = sorted(df_prod["Estate/Kode Vendor"].dropna().unique())
    if not estate_list:
        st.info("Data tidak tersedia.")
        return

    selected_estate = st.selectbox("Pilih Estate", estate_list, key="estate_panen_vs_prod")
    estate_key = normalize_text(selected_estate)

    df_prod_estate = df_prod[df_prod["ESTATE_NORMALIZED"] == estate_key].copy()
    df_panen_estate = df_panen[df_panen["ESTATE_NORMALIZED"] == estate_key].copy()

    divisi_list = sorted(df_prod_estate["Divisi"].dropna().unique())
    if not divisi_list:
        st.info("Divisi tidak tersedia.")
        return

    selected_divisi = st.selectbox("Pilih Divisi", divisi_list, key="divisi_panen_vs_prod")
    divisi_key = normalize_text(selected_divisi)

    df_prod_div = df_prod_estate[df_prod_estate["DIVISI_NORMALIZED"] == divisi_key].copy()
    df_panen_div = df_panen_estate[df_panen_estate["DIVISI_NORMALIZED"] == divisi_key].copy()

    if df_prod_div.empty or df_panen_div.empty:
        st.warning("Data tidak ditemukan.")
        return

    df_prod_div["BULAN"] = df_prod_div["Tanggal Muat"].dt.to_period("M").dt.to_timestamp()
    df_panen_div["BULAN"] = df_panen_div["TANGGAL"].dt.to_period("M").dt.to_timestamp()

    produksi = df_prod_div.groupby("BULAN")["Netto BJR"].sum().reset_index()
    panen = df_panen_div.groupby("BULAN")["BERAT NETTO"].sum().reset_index()

    df_merge = pd.merge(panen, produksi, on="BULAN", how="outer").fillna(0)

    df_merge["BULAN_LABEL"] = format_bulan_indonesia(df_merge["BULAN"])
    df_merge["PANEN_FMT"] = df_merge["BERAT NETTO"].apply(format_angka)
    df_merge["PROD_FMT"] = df_merge["Netto BJR"].apply(format_angka)

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=df_merge["BULAN"],
            y=df_merge["BERAT NETTO"],
            name="Panen",
            mode="lines+markers",
            customdata=df_merge[["BULAN_LABEL", "PANEN_FMT"]],
            hovertemplate="<b>%{customdata[0]}</b><br>Panen: %{customdata[1]} Kg<extra></extra>",
        )
    )

    fig.add_trace(
        go.Scatter(
            x=df_merge["BULAN"],
            y=df_merge["Netto BJR"],
            name="Produksi",
            mode="lines+markers",
            customdata=df_merge[["BULAN_LABEL", "PROD_FMT"]],
            hovertemplate="<b>%{customdata[0]}</b><br>Produksi: %{customdata[1]} Kg<extra></extra>",
        )
    )

    fig.update_layout(
        title=f"Panen vs Produksi - {selected_estate} ({selected_divisi})",
        xaxis_title="Bulan",
        yaxis_title="Kg",
        hovermode="x unified",
    )

    st.plotly_chart(fig, use_container_width=True)


def render_bjr_panen(start_date, end_date):
    df = filter_dataframe_by_date(load_komidel_data(), "TANGGAL AWAL", start_date, end_date)
    st.subheader("BJR Panen Bulanan dari PAS_Komidel")

    estate_list = sorted(df["ESTATE"].dropna().unique())
    if not estate_list:
        st.info("Data BJR Panen tidak tersedia pada range tanggal yang dipilih.")
        return

    selected_estate = st.selectbox("Pilih Estate (BJR Panen)", estate_list, key="estate_bjr_komidel")
    df_estate = df[df["ESTATE"] == selected_estate].copy()

    bjr_divisi_bulanan = (
        df_estate.groupby(["BULAN", "DIVISI"], as_index=False)["BJR PANEN"]
        .mean()
        .sort_values("BULAN")
    )

    fig_div = px.line(
        bjr_divisi_bulanan,
        x="BULAN",
        y="BJR PANEN",
        color="DIVISI",
        markers=True,
        title=f"Rata-rata BJR Panen Bulanan per Divisi - Estate {selected_estate}",
        labels={"BULAN": "Bulan", "BJR PANEN": "BJR Panen", "DIVISI": "Divisi"},
    )
    fig_div.update_layout(hovermode="x unified")
    st.plotly_chart(fig_div, use_container_width=True)

    tabel_div = bjr_divisi_bulanan.copy()
    tabel_div["PERIODE"] = format_bulan_indonesia(tabel_div["BULAN"])
    st.dataframe(
        tabel_div[["PERIODE", "DIVISI", "BJR PANEN"]].style.format({"BJR PANEN": format_angka_desimal}),
        use_container_width=True,
    )

    st.subheader("Grafik BJR Panen per Blok")
    divisi_list = sorted(df_estate["DIVISI"].dropna().unique())
    selected_divisi = st.selectbox("Pilih Divisi (BJR Panen per Blok)", divisi_list, key="divisi_bjr_komidel")
    df_divisi = df_estate[df_estate["DIVISI"] == selected_divisi].copy()

    blok_list = sorted(df_divisi["BLOK"].dropna().unique())
    selected_blok = st.selectbox("Pilih Blok", blok_list, key="blok_bjr_komidel")
    df_blok = df_divisi[df_divisi["BLOK"] == selected_blok].copy()

    bjr_blok_bulanan = (
        df_blok.groupby(["BULAN", "BLOK"], as_index=False)["BJR PANEN"].mean().sort_values("BULAN")
    )
    fig_blok = px.line(
        bjr_blok_bulanan,
        x="BULAN",
        y="BJR PANEN",
        markers=True,
        title=f"BJR Panen Bulanan - Estate {selected_estate} Divisi {selected_divisi} Blok {selected_blok}",
        labels={"BULAN": "Bulan", "BJR PANEN": "BJR Panen"},
    )
    fig_blok.update_traces(
        hovertemplate="<b>%{x|%B %Y}</b><br>BJR Panen: %{y:.2f}<extra></extra>"
    )
    st.plotly_chart(fig_blok, use_container_width=True)

    tabel_blok = bjr_blok_bulanan.copy()
    tabel_blok["PERIODE"] = format_bulan_indonesia(tabel_blok["BULAN"])
    st.dataframe(
        tabel_blok[["PERIODE", "BLOK", "BJR PANEN"]].style.format({"BJR PANEN": format_angka_desimal}),
        use_container_width=True,
    )


def render_panen_per_blok(start_date, end_date):
    st.subheader("Hasil Panen per Blok (Netto BJR per Hektar)")
    df_blok = filter_dataframe_by_date(load_blok_data(), "TANGGAL", start_date, end_date)
    df_area = load_area_data()

    df_grouped = (
        df_blok.groupby(["ESTATE_ACTIVITY", "DIVISI_ACTIVITY", "BLOK_ACTIVITY"])["NETTO BJR"]
        .sum()
        .reset_index()
    )
    df_merge = pd.merge(
        df_grouped,
        df_area[["ESTATE_ACTIVITY", "DIVISI_ACTIVITY", "BLOK_ACTIVITY", "LUAS_HEKTAR"]],
        on=["ESTATE_ACTIVITY", "DIVISI_ACTIVITY", "BLOK_ACTIVITY"],
        how="left",
    )
    df_merge = df_merge[df_merge["LUAS_HEKTAR"] > 0].copy()
    df_merge["NETTO_BJR_PER_HEKTAR"] = df_merge["NETTO BJR"] / df_merge["LUAS_HEKTAR"]

    estate_list = sorted(df_merge["ESTATE_ACTIVITY"].dropna().unique())
    if not estate_list:
        st.info("Data hasil panen per blok tidak tersedia pada range tanggal yang dipilih.")
        return

    selected_estate = st.selectbox("Pilih Estate", estate_list, key="estate_nettobjr_per_hektar")
    df_estate = df_merge[df_merge["ESTATE_ACTIVITY"] == selected_estate].copy()

    divisi_list = sorted(df_estate["DIVISI_ACTIVITY"].dropna().unique())
    selected_divisi = st.selectbox("Pilih Divisi", divisi_list, key="divisi_nettobjr_per_hektar")
    df_div = (
        df_estate[df_estate["DIVISI_ACTIVITY"] == selected_divisi]
        .sort_values("NETTO_BJR_PER_HEKTAR", ascending=False)
        .reset_index(drop=True)
    )

    df_div["NETTO_BJR_PER_HEKTAR_FMT"] = df_div["NETTO_BJR_PER_HEKTAR"].apply(format_angka_desimal)
    df_div["NETTO_BJR_FMT"] = df_div["NETTO BJR"].apply(format_angka)
    df_div["LUAS_HEKTAR_FMT"] = df_div["LUAS_HEKTAR"].apply(format_angka_desimal)

    fig = px.bar(
        df_div,
        x="BLOK_ACTIVITY",
        y="NETTO_BJR_PER_HEKTAR",
        title=f"Netto BJR per Hektar - Estate {selected_estate} Divisi {selected_divisi}",
        labels={"BLOK_ACTIVITY": "Blok", "NETTO_BJR_PER_HEKTAR": "Netto BJR per Hektar"},
        hover_data={
            "NETTO_BJR_PER_HEKTAR": False,
            "NETTO BJR": False,
            "LUAS_HEKTAR": False,
            "NETTO_BJR_PER_HEKTAR_FMT": False,
            "NETTO_BJR_FMT": False,
            "LUAS_HEKTAR_FMT": False,
        },
    )
    fig.update_traces(
        customdata=df_div[["NETTO_BJR_PER_HEKTAR_FMT", "NETTO_BJR_FMT", "LUAS_HEKTAR_FMT"]],
        hovertemplate=(
            "<b>Blok %{x}</b><br>"
            "Netto BJR/Hektar: %{customdata[0]}<br>"
            "Netto BJR: %{customdata[1]}<br>"
            "Luas Hektar: %{customdata[2]}"
            "<extra></extra>"
        ),
    )
    fig.update_layout(xaxis_title="Blok", yaxis_title="Netto BJR per Hektar")
    st.plotly_chart(fig, use_container_width=True)

    st.dataframe(
        df_div[["BLOK_ACTIVITY", "NETTO BJR", "LUAS_HEKTAR", "NETTO_BJR_PER_HEKTAR"]].style.format(
            {
                "NETTO BJR": format_angka,
                "LUAS_HEKTAR": format_angka_desimal,
                "NETTO_BJR_PER_HEKTAR": format_angka_desimal,
            }
        ),
        use_container_width=True,
    )


def render_produktivitas_perawatan(start_date, end_date):
    st.subheader("Dashboard Produktivitas Perawatan")
    df = filter_dataframe_by_date(load_perawatan_data(), "TGL TRX", start_date, end_date)
    df["HASIL_PER_ORANG"] = df["HEKTAR"] / df["TOTAL ANGGOTA"]

    estate_list = sorted(df["ESTATE ACTIVITY"].dropna().unique())
    if not estate_list:
        st.info("Data produktivitas perawatan tidak tersedia pada range tanggal yang dipilih.")
        return

    selected_estate = st.selectbox("Pilih Estate", estate_list, key="estate_filter")
    df_estate = df[df["ESTATE ACTIVITY"] == selected_estate]

    divisi_list = sorted(df_estate["DIVISI ACTIVITY"].dropna().unique())
    selected_divisi = st.selectbox("Pilih Divisi", divisi_list, key="divisi_filter")
    df_divisi = df_estate[df_estate["DIVISI ACTIVITY"] == selected_divisi]

    aktivitas_list = sorted(df_divisi["NAMA AKTIVITAS"].dropna().unique())
    selected_aktivitas = st.selectbox("Pilih Aktivitas", aktivitas_list, key="aktivitas_filter")
    df_filter = df_divisi[df_divisi["NAMA AKTIVITAS"] == selected_aktivitas]

    df_group = (
        df_filter.groupby(["BLOK ACTIVITY"])
        .agg({"HEKTAR": "sum", "TOTAL ANGGOTA": "sum"})
        .reset_index()
    )
    df_group["HASIL_PER_ORANG"] = df_group["HEKTAR"] / df_group["TOTAL ANGGOTA"]

    st.subheader("Produktivitas Per Blok")
    st.dataframe(
        df_group.style.format(
            {
                "HEKTAR": format_angka_desimal,
                "TOTAL ANGGOTA": format_angka,
                "HASIL_PER_ORANG": format_angka_desimal,
            }
        ),
        use_container_width=True,
    )


st.title("Dashboard Produksi Panen dan Produksi")
st.markdown(
    """
    <style>
    section[data-testid="stSidebar"] h2 {
        font-size: 1.45rem;
    }

    section[data-testid="stSidebar"] label,
    section[data-testid="stSidebar"] .stDateInput label,
    section[data-testid="stSidebar"] .stMarkdown,
    section[data-testid="stSidebar"] p {
        font-size: 1.05rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.sidebar.header("Navigasi")
selected_section_label = st.sidebar.selectbox("Pilih analisis", list(SECTION_OPTIONS.keys()))
selected_section = SECTION_OPTIONS[selected_section_label]

st.sidebar.header("Filter Global")
selected_date_range = st.sidebar.date_input(
    "Pilih range waktu",
    value=(DEFAULT_START_DATE, DEFAULT_END_DATE),
    min_value=DEFAULT_START_DATE,
    max_value=DEFAULT_END_DATE,
)
filter_start_date, filter_end_date = resolve_date_range(selected_date_range)
st.sidebar.caption(
    f"Data ditampilkan dari {filter_start_date.strftime('%d/%m/%Y')} sampai {filter_end_date.strftime('%d/%m/%Y')}"
)

if selected_section == "ringkasan_view":
    render_ringkasan_panen(filter_start_date, filter_end_date)
    render_view_panen(filter_start_date, filter_end_date)

elif selected_section == "produksi":
    render_produksi(filter_start_date, filter_end_date)

elif selected_section == "panen_vs_produksi":
    render_panen_vs_produksi(filter_start_date, filter_end_date)

elif selected_section == "bjr_panen":
    render_bjr_panen(filter_start_date, filter_end_date)

elif selected_section == "panen_blok":
    render_panen_per_blok(filter_start_date, filter_end_date)

elif selected_section == "produktif_perawatan":
    render_produktivitas_perawatan(filter_start_date, filter_end_date)