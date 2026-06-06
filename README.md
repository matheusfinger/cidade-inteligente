# 🏙️ Cidade Inteligente - Distribuição de Processos e Dados

Simulação de uma arquitetura distribuída para uma Smart City, desenvolvida para a disciplina de Distribuição de Processos e Dados. O sistema coleta, processa e exibe dados em tempo real utilizando comunicação interlinguagens.

## 📐 Arquitetura do Sistema
O projeto foi construído utilizando uma topologia em estrela baseada no padrão de publicação e descoberta:
- **Gateway (Python):** Atua como servidor central. Utiliza `UDP Multicast` para descoberta de dispositivos, `UDP Unicast` para fluxo contínuo de dados (DataStreams) e `TCP` para roteamento de comandos de controlo. Armazena leituras em SQLite.
- **Sensores de Dados (Python e Java):** - *Estação Meteorológica:* Sensor contínuo (não controlável).
  - *Semáforo e Qualidade do Ar:* Sensores controláveis via TCP.
  - *Câmera de Monitoramento:* Implementada em Java, controlável e responde ao protocolo universal estruturado em Protobuf.
- **Cliente Analítico Web:** Dashboard construído em Streamlit e Pandas que consome a API TCP do Gateway para exibir o estado da rede, gerar agregados estatísticos e enviar comandos de atuação remotos.

## 🛠️ Pré-requisitos
Certifique-se de que tem instalado na sua máquina:
- Python 3.8+
- Java 11+
- Compilador Protocol Buffers (`protoc`)

Instale as dependências do Python executando:
```bash
pip install -r requirements.txt