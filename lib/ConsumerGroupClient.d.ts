import { ConsumerClient } from './ConsumerClient';
export declare class ConsumerGroupClient extends ConsumerClient {
    closing: boolean;
    connecting: boolean;
    connected: boolean;
    reconnecting: boolean;
    consumerInstance: any;
    options: any;
    autoReconnectCount: number;
    kafkaClient: any;
    private topics;
    constructor(options: any, topics: string[]);
    connect(): Promise<void>;
    onConnected(): Promise<any>;
    scheduleReconnect(timeout: number): Promise<any>;
    sendOffsetCommitRequest(commits: any): Promise<{}>;
    addTopics(topics: string | string[]): Promise<{}>;
    pauseTopic(topics: string[]): Promise<void>;
    resumeTopics(topics: string[]): Promise<void>;
    private init(options, topics);
}
