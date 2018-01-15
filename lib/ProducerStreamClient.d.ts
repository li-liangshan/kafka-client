export declare class ProducerStreamClient {
    closing: boolean;
    connecting: boolean;
    connected: boolean;
    streamInstance: any;
    autoReconnectCount: number;
    private options;
    constructor(options?: any, autoReconnectCount?: number);
    connect(): Promise<void>;
    onConnected(): Promise<any>;
    onReconnecting(): Promise<void>;
    close(): Promise<{}>;
    onClosed(): Promise<{}>;
    getProducerStream(): Promise<any>;
    isConnected(): boolean;
}
