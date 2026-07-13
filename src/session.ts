import {CommandType, LinkStatus} from "./messages/gameboy.js";
import {Client} from "./client.js";
import {concatMap, Observable, Subject, Subscription} from "rxjs";
import {v4 as uuidv4} from 'uuid';

type UInt16 = number & { __uint16: true };
type DataArray = [
    UInt16, UInt16, UInt16, UInt16, UInt16, UInt16, UInt16, UInt16,
    UInt16, UInt16, UInt16, UInt16, UInt16, UInt16, UInt16, UInt16,
    UInt16, UInt16, UInt16, UInt16, UInt16, UInt16, UInt16, UInt16,
    UInt16, UInt16, UInt16, UInt16, UInt16, UInt16, UInt16, UInt16,
];

interface DataPacket {
    sequence: number;
    data: DataArray;
}

interface CommandPacket {
    uuid: string;
    command: CommandType;
}

interface StatusPacket {
    uuid: string;
    linkStatus: LinkStatus;
}

interface OutgoingAckablePacket {
    client: Client;
    event: string;
    args?: any;
}

class ClientState {
    public status: LinkStatus = LinkStatus.Empty;
    public subscription: Subscription = new Subscription();
    public packets: Map<number, DataPacket> = new Map();
}

export class Session {

    private clientState: Map<Client, ClientState> = new Map();

    private masterSelected: boolean = false;

    // Room capacity. 2 by default; a multiplayer lobby host raises it to 4
    // via setRoomSize before the joiners join.
    private maxPlayers: number = 2;

    private receivedStati: Set<string> = new Set();

    private clients: Client[] = [];

    private send$: Subject<OutgoingAckablePacket> = new Subject<OutgoingAckablePacket>();

    private started: boolean = false;

    // Set when the host starts a multiplayer lobby. Joins are rejected from
    // then on, since seats are assigned at start. Distinct from `started`
    // (first deviceStatus) so 2-player flows keep their join semantics.
    private lobbyStarted: boolean = false;

    private closeSubject: Subject<Session> = new Subject();

    private ackablePacketSubscription: Subscription;

    /**
     * Observable that emits when one of the following events occurs:
     * - The Link has successfully ended
     * - The clients are evicted.
     * - The last client leaves the session.
     */
    public close$: Observable<Session> = this.closeSubject.asObservable();

    private socketEventHandlers = {

        deviceStatus: (client: Client, statusPacket: StatusPacket) => {
            if (this.receivedStati.has(statusPacket.uuid)) {
                console.warn("Received duplicate status packet");
                return;
            }
            this.receivedStati.add(statusPacket.uuid);
            this.handleStatusMessage(client, statusPacket);
        },

        deviceData: (client: Client, dataPacket: DataPacket) => {
            let receivedPacketMap = this.clientState.get(client)?.packets
            if (!receivedPacketMap) {
                console.log("Received data packet for unknown client");
                return;
            }
            receivedPacketMap.set(dataPacket.sequence, dataPacket);
            this.emitAckedToOppositeSocket(client, "deviceData", dataPacket);
        },

        requestData: (client: Client, missingSequenceNumbers: [number]) => {
            let receivedPacketMap = this.clientState.get(client)?.packets
            if (!receivedPacketMap) {
                console.log("Received data packet for unknown client");
                return;
            }
            missingSequenceNumbers.forEach(seqNum => {
                let packet = receivedPacketMap.get(seqNum)
                if (packet) client.emit("deviceData", packet)
                else console.log("Requested data packet " + seqNum + " not found")
            })
        },

        // --- multiplayer (3-4 player) relay ---

        // Seat-tagged link frame from one adapter, broadcast to every other
        // client in the room.
        relayData: (client: Client, data: any) => {
            this.clients.forEach(other => {
                if (other.id() !== client.id()) other.emit("relayData", data);
            });
        },

        // Host pressed Start: tell every client its seat and the player
        // count so each can enter the link mode in the right role.
        multiLinkStart: (client: Client) => {
            if (this.getSeat(client) !== 0) return; // host only
            const playerCount = this.clients.length;
            if (playerCount < 2) return;
            this.started = true;
            this.lobbyStarted = true;
            console.log("Client " + client.id() + " started the session with " + playerCount + " players");
            this.clients.forEach(other =>
                other.emit("multiLinkStart", { seat: this.getSeat(other), playerCount }));
        },

        // Host sets the room capacity (before joiners join).
        setRoomSize: (client: Client, size: number) => {
            if (this.getSeat(client) !== 0) return; // host only
            this.setMaxPlayers(size);
        }
    };

    constructor(private sessionId: string) {
        this.ackablePacketSubscription = this.send$.pipe(
            concatMap((packet: OutgoingAckablePacket) =>
                packet.client.emitWithRetry<boolean>(packet.event, packet.args)
                    .catch(err => {
                        console.error("Ack failed after retries:", err);
                        this.ackablePacketSubscription.unsubscribe()
                        this.evict();
                        return Promise.resolve();
                    })
            )
        ).subscribe();
    }

    /**
     * Check if the session has started, meaning the first status packet has been received.
     * @returns {boolean} - true if the session has started, false otherwise.
     */
    hasStarted(): boolean { return this.started; }

    /** True once the host started a multiplayer lobby. */
    hasLobbyStarted(): boolean { return this.lobbyStarted; }

    private makeCommand(command: CommandType) : CommandPacket {
        return {uuid: uuidv4(), command: command}
    }

    private handleStatusMessage(client: Client, statusPacket: StatusPacket): void {
        console.log("Client " + client.id() + " has send status and was received by server. Status: " + LinkStatus[statusPacket.linkStatus]);

        this.started = true;
        let clientState = this.clientState.get(client)!

        switch (statusPacket.linkStatus) {
            case LinkStatus.AwaitMode:
                if (!this.masterSelected) {
                    client.emit("deviceCommand", this.makeCommand(CommandType.SetModeMaster))
                    this.masterSelected = true;
                }
                else {
                    client.emit("deviceCommand", this.makeCommand(CommandType.SetModeSlave))
                }
                break

            case LinkStatus.AwaitModeEmulator:
                client.emit("deviceCommand", this.makeCommand(CommandType.SetModeSlave))
                break

            case LinkStatus.HandshakeReceived:
                clientState.status = LinkStatus.HandshakeReceived;

                const allHandshakesReceived = [...this.clientState.values()]
                    .every(state => state.status === LinkStatus.HandshakeReceived);

                if (allHandshakesReceived) {
                    client.emit("deviceCommand", this.makeCommand(CommandType.StartHandshake))
                    this.emitToOppositeSocket(client,"deviceCommand", this.makeCommand(CommandType.StartHandshake));
                }
                break

            case LinkStatus.HandshakeFinished:
                clientState.status = LinkStatus.HandshakeFinished;
                break

            case LinkStatus.LinkConnected:
                clientState.status = LinkStatus.LinkConnected;
                this.emitToOppositeSocket(client, "deviceCommand", this.makeCommand(CommandType.ConnectLink))
                break;

            case LinkStatus.LinkReconnecting:
                clientState.status = LinkStatus.LinkReconnecting;
                break;

            case LinkStatus.LinkClosed:
                clientState.status = LinkStatus.LinkClosed;
                const allLinksClosed = [...this.clientState.values()]
                    .every(state => state.status === LinkStatus.LinkClosed);

                if (allLinksClosed) {
                    console.log("All links closed. Session will be closed");
                    setTimeout(() => {
                        this.evict();
                    }, 2000);
                }
                break;
        }
    }

    /**
     * Get the ID of the session.
     * @returns {string} - The ID of the session.
     */
    id(): string {
        return this.sessionId;
    }

    /**
     * Check if the session is full.
     * @returns {boolean} - true if the session is full, false otherwise.
     */
    isFull(): boolean {
        return this.clients.length >= this.maxPlayers;
    }

    /** Set the room capacity. Clamped to 2-4. */
    setMaxPlayers(n: number) {
        this.maxPlayers = Math.min(Math.max(Math.floor(n), 2), 4);
    }

    /** The client's seat (index in join order). 0 = host. -1 if not in session. */
    getSeat(client: Client): number {
        return this.clients.findIndex(c => c.id() === client.id());
    }

    /**
     * Check if the session is empty.
     * @returns {boolean} - true if the session is empty, false otherwise.
     */
    isEmpty(): boolean {
        return this.clients.length === 0;
    }

    /**
     * Add a client to the session. Emits a "partnerJoined" event to the other client.
     * @param client
     */
    enter(client: Client) : boolean {
        if (this.isFull()) {
            console.warn("Session is full");
            return false;
        }
        this.clients.push(client);
        this.clientState.set(client, new ClientState());
        Object.entries(this.socketEventHandlers).forEach(([event, handler]) => {
            this.clientState.get(client)?.subscription.add(client.fromEvent<any>(event).subscribe((data: any) => handler(client, data)));
        });
        this.emitToOppositeSocket(client, "partnerJoined");
        client.inSession(true)
        return true;
    }

    /**
     * Remove a client from the session. Emits a "partnerLeft" event to the other client.
     * @param client
     */
    leave(client: Client) {
        const index = this.clients.findIndex(clientToRemove => clientToRemove.id() === client.id());
        if (index < 0){
            console.warn("Client " + client.id() + " tried to leave session but was not in one");
            return
        }
        if (this.isFull() && this.hasStarted())
        {
            this.evict()
        }
        else if (index === 0 && !this.hasStarted() && this.clients.length > 1
                 && this.maxPlayers > 2)
        {
            // The host left a multiplayer lobby before starting. Seats would
            // shift and no remaining client knows it became the host, so
            // close the room. 2-player sessions keep the original
            // partnerLeft behaviour.
            console.log("Host left session " + this.sessionId + " before starting, closing it");
            this.evict();
        }
        else {
            this.emitToOppositeSocket(client, "partnerLeft");
            this.removeClient(client);
        }
    }

    /**
     * Remove a client from the session.
     * @param client
     */
    private removeClient(client: Client) {
        const index = this.clients.findIndex(clientToRemove => clientToRemove.id() === client.id());
        this.clientState.get(client)?.subscription.unsubscribe();
        this.clients.splice(index, 1);
        console.log("Client " + client.id() + " left session");
        client.inSession(false);
        if (this.isEmpty()) this.closeSubject.next(this);
    }

    /**
     * Remove all clients from the session. Emits a "sessionClose" event to all clients
     */
    private evict() {
        for (let i = this.clients.length - 1; i >= 0; i--) {
            this.clients[i].emit("sessionClose")
            this.removeClient(this.clients[i]);
        }
    }

    /**
     * Emit an event to the opposite socket of the given client. Save to call when only one client is in the session.
     * @param client
     * @param event
     * @param arg
     */
    private emitToOppositeSocket(client: Client, event: string, arg?: any) {
        // Broadcast to every other client. For a 2-client session this is the
        // single opposite socket (unchanged behaviour); for 3-4 it fans out.
        this.clients.forEach(other => {
            if (other.id() !== client.id()) {
                console.log('Emitting to Client ' + other.id() + ': ' + event);
                other.emit(event, arg);
            }
        });
    }

    /**
     * Emit an event to the opposite socket of the given client. Save to call when only one client is in the session.
     * @param client
     * @param event
     * @param arg
     */
    private emitAckedToOppositeSocket(client: Client, event: string, args?: any) {
        // Ackable, ordered fan-out to every other client (single opposite for a
        // 2-client session).
        this.clients.forEach(other => {
            if (other.id() !== client.id()) this.queueAckablePacket(other, event, args);
        });
    }

    /**
     * Queue and ackable Event to preserve order.
     * @param client
     * @param event
     * @param args
     * @private
     */
    private queueAckablePacket(client: Client, event: string, args?: any) {
        this.send$.next({client: client, event: event, args: args});
    }
}