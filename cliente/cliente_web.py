import streamlit as st
import socket
import sys
import os
import json
import pandas as pd

# Ajuste do path para o Protobuf
pasta_raiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, pasta_raiz)
from proto import smart_city_pb2

GATEWAY_IP = '127.0.0.1'
GATEWAY_TCP_PORT = 7000

def send_request(req):
    """Envia requisição TCP para o Gateway e retorna a resposta."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(3.0)
        s.connect((GATEWAY_IP, GATEWAY_TCP_PORT))
        s.sendall(req.SerializeToString())
        data = s.recv(4096)
        s.close()
        
        if not data: return None
        resp = smart_city_pb2.ClientResponse()
        resp.ParseFromString(data)
        return resp
    except Exception as e:
        st.error(f"Erro de conexão com o Gateway: {e}")
        return None

# --- Buscar e tratar a lista de dispositivos ativos e inativos ---
def get_status_rede():
    req = smart_city_pb2.ClientRequest()
    req.type = smart_city_pb2.ClientRequest.GET_STATUS
    req.target_device_id = "todos"
    
    resp = send_request(req)
    if not resp or resp.message == "Nenhum dispositivo ativo.":
        return [], []
        
    ativos = []
    inativos = []
    
    # Recorta a string formatada "- Nome | Tipo | Estado: X | TCP"
    for linha in resp.message.split('\n'):
        if linha.startswith('- '):
            partes = linha.split(' | ')
            nome = partes[0].replace('- ', '').strip()
            
            # Pega a parte do estado (ex: "Estado: ATIVO") e limpa o texto
            estado = partes[2].replace('Estado: ', '').strip()
            
            if estado == "ATIVO":
                ativos.append(nome)
            else:
                inativos.append(nome)
                
    return ativos, inativos

# --- CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(page_title="Smart City Dashboard", page_icon="🏙️", layout="wide")
st.title("Smart City - Central Analítica")

# Busca os dispositivos ativos e inativos em tempo real
ativos, inativos = get_status_rede()
# Junta tudo para manter as caixas de seleção da barra lateral funcionando
todos_dispositivos = ativos + inativos

# --- BARRA LATERAL: CONTROLES ---
st.sidebar.header("Controle de Dispositivos")

alvo = st.sidebar.selectbox(
    "Selecione o Dispositivo Alvo", 
    options=[""] + todos_dispositivos, 
    format_func=lambda x: "Selecione..." if x == "" else x
)

comando_op = st.sidebar.selectbox("Comando", ["Nenhum", "Ligar", "Desligar", "Alterar Frequência"])
parametro = 0.0

if comando_op == "Alterar Frequência":
    parametro = st.sidebar.number_input("Nova Frequência (s)", min_value=1.0, value=5.0)

if st.sidebar.button("Enviar Comando"):
    if alvo and comando_op != "Nenhum":
        cmd = smart_city_pb2.Command()
        if comando_op == "Ligar": cmd.action = smart_city_pb2.Command.TURN_ON
        elif comando_op == "Desligar": cmd.action = smart_city_pb2.Command.TURN_OFF
        elif comando_op == "Alterar Frequência":
            cmd.action = smart_city_pb2.Command.CHANGE_FREQ
            cmd.parameter = parametro
            
        req = smart_city_pb2.ClientRequest()
        req.type = smart_city_pb2.ClientRequest.SEND_COMMAND
        req.target_device_id = alvo
        req.command_payload.CopyFrom(cmd)
        
        resp = send_request(req)
        if resp and resp.success:
            st.sidebar.success(f"Comando enviado: {resp.message}")
        else:
            st.sidebar.error(f"Falha: {resp.message if resp else 'Sem resposta'}")

# --- CORPO PRINCIPAL ---
col1, col2 = st.columns([1, 2])

# Coluna 1: Status da Rede
with col1:
    st.subheader("📡 Status da Rede")
    if st.button("Atualizar Rede", use_container_width=True):
        st.rerun()
    
    if not ativos and not inativos:
        st.warning("Nenhum sensor conectado à rede.")
    else:
        # Bloco de Ativos (Verde)
        if ativos:
            st.markdown("🟢 **Ativos** 🟢")
            for d in ativos:
                st.success(d)
                
        # Bloco de Inativos (Vermelho/Amarelo)
        if inativos:
            st.markdown("🔴 **Inativos** 🔴")
            for d in inativos:
                st.error(d)

# Coluna 2: Gráficos de Séries Temporais
with col2:
    st.subheader("Séries Temporais (Histórico)")
    
    # Tratamento especial para a Estação Meteorológica que tem 2 gráficos
    opcoes_grafico = []
    for d in todos_dispositivos:
        if "WeatherStation" in d:
            opcoes_grafico.extend([f"{d}:temperatura", f"{d}:umidade"])
        else:
            opcoes_grafico.append(d)
            
    # Selectbox dinâmico para os gráficos
    grafico_id = st.selectbox("Selecione a Métrica para visualizar", options=opcoes_grafico)
    
    if st.button("Buscar e Plotar Dados", type="primary", use_container_width=True):
        if grafico_id:
            req = smart_city_pb2.ClientRequest()
            req.type = smart_city_pb2.ClientRequest.GET_HISTORY
            req.target_device_id = grafico_id
            
            resp = send_request(req)
            if resp and resp.message and resp.message != "[]":
                dados = json.loads(resp.message)
                
                df = pd.DataFrame(dados)
                df['datetime'] = pd.to_datetime(df['timestamp'], unit='s')
                # Ajusta para o fuso horário local subtraindo 3 horas do UTC
                df['datetime'] = df['datetime'] - pd.Timedelta(hours=3)
                df = df.set_index('datetime')
                
                # Plota o gráfico
                st.line_chart(df['value'])
                
                # Métricas em destaque embaixo do gráfico
                m1, m2, m3 = st.columns(3)
                m1.metric("Leituras Analisadas", len(df))
                m2.metric("Média do Período", f"{df['value'].mean():.2f}")
                # Calcula o desvio padrão (se houver mais de 1 leitura para evitar erro matemático)
                desvio = df['value'].std() if len(df) > 1 else 0.0
                m3.metric("Desvio Padrão", f"{desvio:.2f}")
                m3.metric("Última Leitura", f"{df['value'].iloc[0]:.2f}")
            else:
                st.warning("Aguardando dados... O sensor ainda não enviou leituras suficientes.")