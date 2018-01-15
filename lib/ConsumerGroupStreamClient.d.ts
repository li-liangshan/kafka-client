export declare class ConsumerGroupStreamClient {
    closing: boolean;
    connecting: boolean;
    connected: boolean;
    groupInstance: any;
    options: any;
    autoReconnectCount: number;
    kafkaClient: any;
    private topics;
    constructor(options: any, topics: string | string[]);
    connect(): Promise<any>;
    close(): Promise<any>;
    getConsumerGroupStream(): Promise<any>;
    private onConnected();
    private onReconnecting();
    private init(options, topics);
    private onClosed();
}
