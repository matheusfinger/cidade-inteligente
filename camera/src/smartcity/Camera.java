package smartcity;

import java.io.IOException;
import java.net.*;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Câmera de monitoramento urbano — fonte contínua controlável.
 *
 * Comportamento:
 *  - Discovery : escuta multicast UDP e responde com DiscoveryResponse
 *  - Dados     : envia DataStream UDP periódico (pessoas detectadas no frame)
 *  - Comandos  : aceita TURN_ON, TURN_OFF e CHANGE_FREQ via TCP
 */
public class Camera {

    // ── Configurações ─────────────────────────────────────────────────────────
    private static final String MULTICAST_GROUP  = "224.0.0.1";
    private static final int    MULTICAST_PORT   = 5000;
    private static final String GATEWAY_IP       = "127.0.0.1";
    private static final int    GATEWAY_PORT     = 5001;
    private static final int    DATA_PORT        = 5002;
    private static final int    TCP_PORT         = 6004;
    private static final String DEVICE_ID        = "Camera-PracaCentral";
    private static final int    DEFAULT_INTERVAL = 12;   // segundos

    // ── Estado (modificável por comandos TCP) ─────────────────────────────────
    private final AtomicBoolean active        = new AtomicBoolean(true);
    private final AtomicBoolean running       = new AtomicBoolean(true);
    private final AtomicInteger sendInterval  = new AtomicInteger(DEFAULT_INTERVAL);

    // ── Simulação ─────────────────────────────────────────────────────────────
    private final Random rng = new Random();
    private double pessoasBase = 15.0;

    // ── Entry point ───────────────────────────────────────────────────────────
    public static void main(String[] args) throws InterruptedException {
        Camera cam = new Camera();

        Thread tDiscovery = new Thread(cam::listenForDiscover, "discovery");
        Thread tCommands  = new Thread(cam::listenForCommands, "commands");

        tDiscovery.setDaemon(true);
        tCommands.setDaemon(true);

        tDiscovery.start();
        tCommands.start();

        // Pequeno delay para os sockets abrirem antes de enviar dados
        Thread.sleep(500);

        // Registra shutdown hook para Ctrl+C
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            cam.running.set(false);
            System.out.println("\n[" + DEVICE_ID + "] Desligado.");
        }));

        cam.sendDataLoop();
    }

    // ── Discovery: escuta DISCOVER e responde ─────────────────────────────────
    private void listenForDiscover() {
        try (MulticastSocket sock = new MulticastSocket(MULTICAST_PORT)) {
            sock.setReuseAddress(true);
            // SO_REUSEPORT equivalente em Java: setReuseAddress já cobre na maioria dos SO
            InetAddress group = InetAddress.getByName(MULTICAST_GROUP);
            sock.joinGroup(group);

            System.out.printf("[%s] Escutando multicast %s:%d...%n",
                    DEVICE_ID, MULTICAST_GROUP, MULTICAST_PORT);

            byte[] buf = new byte[1024];

            while (running.get()) {
                DatagramPacket pkt = new DatagramPacket(buf, buf.length);
                sock.receive(pkt);

                try {
                    SmartCity.DiscoveryRequest req =
                            SmartCity.DiscoveryRequest.parseFrom(
                                    java.util.Arrays.copyOf(pkt.getData(), pkt.getLength()));

                    System.out.printf("[%s] DISCOVER de '%s' — respondendo...%n",
                            DEVICE_ID, req.getGatewayId());

                    // Monta DiscoveryResponse
                    SmartCity.DiscoveryResponse resp = SmartCity.DiscoveryResponse.newBuilder()
                            .setDeviceId(DEVICE_ID)
                            .setType(SmartCity.DeviceType.CAMERA)
                            .setIp("127.0.0.1")
                            .setTcpPort(TCP_PORT)
                            .setInitialState(active.get() ? "ATIVO" : "INATIVO")
                            .build();

                    byte[] respBytes = resp.toByteArray();

                    // Responde sempre para GATEWAY_IP (não para pkt.getAddress())
                    // pkt.getAddress() em multicast local pode ser o IP do roteador,
                    // não o IP real do gateway.
                    try (DatagramSocket rs = new DatagramSocket()) {
                        rs.send(new DatagramPacket(
                                respBytes, respBytes.length,
                                InetAddress.getByName(GATEWAY_IP), GATEWAY_PORT));
                    }

                } catch (Exception e) {
                    System.err.printf("[%s] Erro ao processar DISCOVER: %s%n",
                            DEVICE_ID, e.getMessage());
                }
            }

            sock.leaveGroup(group);

        } catch (IOException e) {
            System.err.printf("[%s] Erro no listener multicast: %s%n",
                    DEVICE_ID, e.getMessage());
        }
    }

    // ── Comandos TCP ──────────────────────────────────────────────────────────
    private void listenForCommands() {
        try (ServerSocket srv = new ServerSocket(TCP_PORT)) {
            srv.setReuseAddress(true);
            srv.setSoTimeout(1000);   // timeout para verificar running periodicamente

            System.out.printf("[%s] Aguardando comandos TCP na porta %d...%n",
                    DEVICE_ID, TCP_PORT);

            while (running.get()) {
                try {
                    Socket conn = srv.accept();
                    // Cada conexão tratada em thread separada
                    Thread t = new Thread(() -> handleCommand(conn), "cmd-handler");
                    t.setDaemon(true);
                    t.start();
                } catch (SocketTimeoutException ignored) {
                    // loop para verificar running
                }
            }
        } catch (IOException e) {
            if (running.get()) {
                System.err.printf("[%s] Erro no servidor TCP: %s%n",
                        DEVICE_ID, e.getMessage());
            }
        }
    }

    private void handleCommand(Socket conn) {
        try (conn) {
            conn.setSoTimeout(2000);
            java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
            byte[] tmp = new byte[1024];
            int n;
            try {
                while ((n = conn.getInputStream().read(tmp)) != -1) {
                    baos.write(tmp, 0, n);
                }
            } catch (SocketTimeoutException ignored) {}
            byte[] buf = baos.toByteArray();
            if (buf.length == 0) return;

            SmartCity.Command cmd = SmartCity.Command.parseFrom(buf);

            switch (cmd.getAction()) {
                case TURN_ON:
                    active.set(true);
                    System.out.printf("[%s] Comando: ATIVADO%n", DEVICE_ID);
                    break;

                case TURN_OFF:
                    active.set(false);
                    System.out.printf("[%s] Comando: DESATIVADO%n", DEVICE_ID);
                    break;

                case CHANGE_FREQ:
                    int novoIntervalo = Math.max(1, (int) cmd.getParameter());
                    sendInterval.set(novoIntervalo);
                    System.out.printf("[%s] Comando: frequência → %ds%n",
                            DEVICE_ID, novoIntervalo);
                    break;

                case SET_THRESHOLD:
                    // Câmera não tem limiar de alerta, mas aceita o comando sem erros
                    System.out.printf("[%s] Comando SET_THRESHOLD ignorado (câmera não usa limiar)%n",
                            DEVICE_ID);
                    break;

                default:
                    System.out.printf("[%s] Comando desconhecido recebido%n", DEVICE_ID);
            }

        } catch (IOException e) {
            System.err.printf("[%s] Erro ao processar comando: %s%n",
                    DEVICE_ID, e.getMessage());
        }
    }

    // ── Simulação de pessoas detectadas ──────────────────────────────────────
    /**
     * Random walk com padrão diurno:
     *   - madrugada (0h-6h):  0-5 pessoas
     *   - manhã (7h-9h):      20-60 (pico)
     *   - tarde (10h-16h):    10-30
     *   - tarde/noite (17h-19h): 25-50 (segundo pico)
     *   - noite (20h-23h):    5-15
     */
    private double nextPessoas() {
        int hora = java.time.LocalTime.now().getHour();
        double base;

        if (hora >= 7 && hora <= 9)        base = rng.nextDouble() * 40 + 20;
        else if (hora >= 17 && hora <= 19) base = rng.nextDouble() * 25 + 25;
        else if (hora >= 0 && hora <= 5)   base = rng.nextDouble() * 5;
        else if (hora >= 20)               base = rng.nextDouble() * 10 + 5;
        else                               base = rng.nextDouble() * 20 + 10;

        // Random walk suave
        pessoasBase += (rng.nextDouble() - 0.5) * 4;
        pessoasBase  = Math.max(0, Math.min(pessoasBase, base * 1.2));

        return Math.max(0, Math.round(pessoasBase));
    }

    // ── Loop de envio de dados UDP ────────────────────────────────────────────
    private void sendDataLoop() {
        try (DatagramSocket dataSock = new DatagramSocket()) {
            System.out.printf("[%s] Enviando dados a cada %ds...%n",
                    DEVICE_ID, sendInterval.get());

            while (running.get()) {
                if (!active.get()) {
                    System.out.printf("[%s] Câmera desativada — aguardando reativação...%n",
                            DEVICE_ID);
                    Thread.sleep(2000);
                    continue;
                }

                double pessoas = nextPessoas();

                SmartCity.DataStream stream = SmartCity.DataStream.newBuilder()
                        .setDeviceId(DEVICE_ID)
                        .setValue((float) pessoas)
                        .setTimestamp(System.currentTimeMillis() / 1000L)
                        .build();

                byte[] data = stream.toByteArray();
                dataSock.send(new DatagramPacket(
                        data, data.length,
                        InetAddress.getByName(GATEWAY_IP), DATA_PORT));

                System.out.printf("[%s] Pessoas detectadas: %.0f%n", DEVICE_ID, pessoas);

                Thread.sleep(sendInterval.get() * 1000L);
            }

        } catch (IOException | InterruptedException e) {
            if (running.get()) {
                System.err.printf("[%s] Erro no loop de dados: %s%n",
                        DEVICE_ID, e.getMessage());
            }
        }
    }
}
