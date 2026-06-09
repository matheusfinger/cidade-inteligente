package smartcity;

import java.io.IOException;
import java.net.*;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class Camera {

    private static final String MULTICAST_GROUP  = "224.0.0.1";
    private static final int    MULTICAST_PORT   = 5000;
    private static final String GATEWAY_IP       = "127.0.0.1";
    private static final int    GATEWAY_PORT     = 5001;
    private static final int    DATA_PORT        = 5002;
    private static final String DEVICE_ID        = "Camera-PracaCentral";
    private static final int    DEFAULT_INTERVAL = 12;

    private final AtomicBoolean active       = new AtomicBoolean(true);
    private final AtomicBoolean running      = new AtomicBoolean(true);
    private final AtomicInteger sendInterval = new AtomicInteger(DEFAULT_INTERVAL);

    // porta TCP real atribuída pelo SO após bind — anunciada no DiscoveryResponse
    private final AtomicInteger tcpPortReal  = new AtomicInteger(0);

    private final Random rng = new Random();
    private double pessoasBase = 15.0;

    public static void main(String[] args) throws InterruptedException {
        Camera cam = new Camera();

        // Servidor TCP sobe PRIMEIRO para preencher tcpPortReal antes do discovery
        Thread tCommands = new Thread(cam::listenForCommands, "commands");
        tCommands.setDaemon(true);
        tCommands.start();

        // Aguarda o bind acontecer e tcpPortReal ser preenchido
        while (cam.tcpPortReal.get() == 0) {
            Thread.sleep(50);
        }

        Thread tDiscovery = new Thread(cam::listenForDiscover, "discovery");
        tDiscovery.setDaemon(true);
        tDiscovery.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            cam.running.set(false);
            System.out.println("\n[" + DEVICE_ID + "] Desligado.");
        }));

        cam.sendDataLoop();
    }

    // ── Discovery ─────────────────────────────────────────────────────────────
    private void listenForDiscover() {
        try (MulticastSocket sock = new MulticastSocket(MULTICAST_PORT)) {
            sock.setReuseAddress(true);
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

                    System.out.printf("[%s] DISCOVER de '%s' — respondendo na porta TCP %d...%n",
                            DEVICE_ID, req.getGatewayId(), tcpPortReal.get());

                    SmartCity.DiscoveryResponse resp = SmartCity.DiscoveryResponse.newBuilder()
                            .setDeviceId(DEVICE_ID)
                            .setType(SmartCity.DeviceType.CAMERA)
                            .setIp("127.0.0.1")
                            .setTcpPort(tcpPortReal.get())   // porta dinâmica real
                            .setInitialState(active.get() ? "ATIVO" : "INATIVO")
                            .build();

                    try (DatagramSocket rs = new DatagramSocket()) {
                        byte[] respBytes = resp.toByteArray();
                        rs.send(new DatagramPacket(respBytes, respBytes.length,
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
        // porta 0 → SO escolhe uma porta livre
        try (ServerSocket srv = new ServerSocket(0)) {
            srv.setReuseAddress(true);
            srv.setSoTimeout(1000);

            // registra a porta real para o discovery anunciar
            tcpPortReal.set(srv.getLocalPort());
            System.out.printf("[%s] Comandos TCP na porta %d (dinâmica)...%n",
                    DEVICE_ID, tcpPortReal.get());

            while (running.get()) {
                try {
                    Socket conn = srv.accept();
                    new Thread(() -> handleCommand(conn), "cmd-handler").start();
                } catch (SocketTimeoutException ignored) {}
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
            // buf vazio = TURN_ON (valor default 0 não é serializado pelo proto3)
            SmartCity.Command cmd = SmartCity.Command.parseFrom(buf);

            System.out.printf("[%s] Comando: action=%s parameter=%.1f%n",
                    DEVICE_ID, cmd.getAction(), cmd.getParameter());

            switch (cmd.getAction()) {
                case TURN_ON:
                    active.set(true);
                    System.out.printf("[%s] ATIVADO%n", DEVICE_ID);
                    break;
                case TURN_OFF:
                    active.set(false);
                    System.out.printf("[%s] DESATIVADO%n", DEVICE_ID);
                    break;
                case CHANGE_FREQ:
                    int novo = Math.max(1, (int) cmd.getParameter());
                    sendInterval.set(novo);
                    System.out.printf("[%s] Frequência → %ds%n", DEVICE_ID, novo);
                    break;
                case SET_THRESHOLD:
                    System.out.printf("[%s] SET_THRESHOLD ignorado%n", DEVICE_ID);
                    break;
                default:
                    System.out.printf("[%s] Comando desconhecido%n", DEVICE_ID);
            }

        } catch (IOException e) {
            System.err.printf("[%s] Erro ao processar comando: %s%n",
                    DEVICE_ID, e.getMessage());
        }
    }

    // ── Simulação ─────────────────────────────────────────────────────────────
    private double nextPessoas() {
        int hora = java.time.LocalTime.now().getHour();
        double base;
        if      (hora >= 7  && hora <= 9)  base = rng.nextDouble() * 40 + 20;
        else if (hora >= 17 && hora <= 19) base = rng.nextDouble() * 25 + 25;
        else if (hora >= 0  && hora <= 5)  base = rng.nextDouble() * 5;
        else if (hora >= 20)               base = rng.nextDouble() * 10 + 5;
        else                               base = rng.nextDouble() * 20 + 10;
        pessoasBase += (rng.nextDouble() - 0.5) * 4;
        pessoasBase  = Math.max(0, Math.min(pessoasBase, base * 1.2));
        return Math.max(0, Math.round(pessoasBase));
    }

    // ── Envio de dados ────────────────────────────────────────────────────────
    private void sendDataLoop() {
        try (DatagramSocket dataSock = new DatagramSocket()) {
            System.out.printf("[%s] Enviando dados a cada %ds...%n",
                    DEVICE_ID, sendInterval.get());

            while (running.get()) {
                if (!active.get()) {
                    System.out.printf("[%s] Desativada — aguardando reativação...%n", DEVICE_ID);
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
                dataSock.send(new DatagramPacket(data, data.length,
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