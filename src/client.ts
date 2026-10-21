import {Socket} from "socket.io";
import {SessionManager} from "./sessionManager.js";
import {BehaviorSubject, fromEvent, Observable, switchMap} from 'rxjs';
import {v4 as uuidv4} from 'uuid';

/**
 * A received event: its payload, and the callback the sender attached for an
 * acknowledgement, if it attached one.
 */
export interface Received<T> {
    data: T;
    ack?: (value: unknown) => void;
}

export class Client {

    private currentlyInSession: boolean = false;
    // What this client speaks. "acking": it has sent an event with an ack
    // callback, so it is a Celio-Client that acks and retries (since b057315)
    // and expects the same of the server. "plain": it sent device traffic
    // without one, an earlier client. Until either, "unknown".
    private protocol: "unknown" | "acking" | "plain" = "unknown";
    private socket$: BehaviorSubject<Socket>;
    private eventHandlers = {

        sessionCreate: (sessionId: string, responseHandler: any) => {
            const sessionState = this.sessionManager.createSession(this);
            if (sessionState.isOk) console.log('Client ' + this.clientId + ` created session with id ` + sessionState.value.id);
            responseHandler(sessionState);
        },

        sessionJoin: (sessionId: string, responseHandler: any) => {
            console.log(this.clientId + ` Client wants to join session ` + sessionId);
            const sessionState = this.sessionManager.enterSession(this, sessionId);
            responseHandler(sessionState);
        },

        sessionLeft: () => {
            console.log('Client ' + this.clientId + ` tries to leave session`);
            this.sessionManager.leaveSession(this);
        },

        disconnect: () => {
            console.log('Client ' + this.clientId + ` disconnected`);
            setTimeout(() => this.checkDisconnect(), 5000);
        }
    };

    constructor(private clientId: string, private socket: Socket, private sessionManager: SessionManager,
                private removeCb: (clientId: string) => void) {
        Object.entries(this.eventHandlers).forEach(([event, handler]) => {
            socket.on(event, handler);
        });
        this.socket$ = new BehaviorSubject(socket);
    }

    reconnect(socket: Socket) {
        this.socket = socket;
        Object.entries(this.eventHandlers).forEach(([event, handler]) => {
            socket.on(event, handler);
        });
        this.socket$.next(socket);
        console.warn('Client ' + this.clientId + ` reconnected`);
    }

    id (): string {
        return this.clientId;
    }

    inSession(entered: boolean) {
        return this.currentlyInSession = entered;
    }

    checkDisconnect() {
        if (this.socket.disconnected){
            if (this.currentlyInSession) this.sessionManager.leaveSession(this)
            this.removeCb(this.clientId);
        }
        else console.warn('Client ' + this.clientId + ` recovered from disconnect`);
    }

    /**
     * Create an observable from a socket.io event.
     * @param event - The name of the event to listen for.
     */
    fromEvent<T>(event: string): Observable<T> {
        return this.socket$.pipe(
            switchMap((socket: Socket) => fromEvent<T>(socket, event))
        );
    }

    /**
     * Like fromEvent, but keeps the payload and the sender's ack callback apart.
     * (rxjs' fromEvent hands a listener's arguments over as an array when there
     * is more than one, so an emit with an ack would arrive as [payload, ack].)
     * @param event - The name of the event to listen for.
     */
    fromEventWithAck<T>(event: string): Observable<Received<T>> {
        return this.socket$.pipe(
            switchMap((socket: Socket) => new Observable<Received<T>>(observer => {
                const handler = (...args: any[]) => {
                    const ack = typeof args[args.length - 1] === "function" ? args.pop() : undefined;
                    if (ack) this.protocol = "acking";
                    else if (this.protocol === "unknown") this.protocol = "plain";
                    observer.next({data: args[0], ack});
                };
                socket.on(event, handler);
                return () => { socket.off(event, handler); };
            }))
        );
    }

    /**
     * Whether this client acks what it receives, and wants deviceData batched
     * as an array (true), or is an earlier client (false).
     */
    acksEvents(): boolean {
        return this.protocol === "acking";
    }

    /**
     * Send a session event (partnerJoined, partnerLeft, sessionClose). A client
     * that acks, or one not yet known not to, gets it acked and retried with a
     * uuid to drop duplicates by; an earlier client, a plain emit, as before.
     * (A client is told apart only once it sends device traffic, so an earlier
     * one can see partnerJoined/partnerLeft more than once while it waits for
     * a partner; it handles them as states, so that is harmless.)
     * @param event
     */
    emitSessionEvent(event: string) {
        if (this.protocol === "plain") {
            this.socket.emit(event);
            return;
        }
        this.emitWithRetry(event, {uuid: uuidv4()})
            .catch(() => console.warn("Client " + this.clientId + " did not ack " + event));
    }

    /**
     * Emit an event to the client.
     * @param event - The name of the event to emit.
     * @param arg - Optional arguments to pass to the event handler.
     */
    emit(event: string, arg?: any) {
        this.socket.emit(event, arg);
    }

    /**
     * Emit an event to the server with retry logic.
     * @param event
     * @param data
     * @param retries
     * @param timeout
     * @param backoff
     */
    emitWithRetry<R>(event: string, data?: any, {
        retries = 5,
        timeout = 1000,
        backoff = 100  // ms added per retry
    } = {}) {
        return new Promise((resolve, reject) => {
            let attempt = 0;

            const tryEmit = () => {
                attempt++;

                this.socket.timeout(timeout).emit(event, data, (err: Error | null, ackValue: R) => {
                    if (!err) {
                        resolve(true);
                        return;
                    }

                    if (attempt > retries) {
                        reject(new Error("Max retries reached"));
                        return;
                    }

                    setTimeout(tryEmit, backoff * attempt);
                });
            };

            tryEmit();
        });
    }
}