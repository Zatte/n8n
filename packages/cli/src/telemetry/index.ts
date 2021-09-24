/* eslint-disable @typescript-eslint/no-unsafe-call */
/* eslint-disable @typescript-eslint/no-unsafe-member-access */
import TelemetryClient = require('@rudderstack/rudder-sdk-node');
import { IDataObject } from 'n8n-workflow';
import config = require('../../config');

interface IExecutionCountsBufferItem {
	manual_success_count: number;
	manual_error_count: number;
	prod_success_count: number;
	prod_error_count: number;
}

interface IExecutionCountsBuffer {
	[workflowId: string]: IExecutionCountsBufferItem;
}

export class Telemetry {
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	private client?: any;

	private instanceId: string;

	private pulseIntervalReference: NodeJS.Timeout;

	private executionCountsBuffer: IExecutionCountsBuffer = {};

	constructor(instanceId: string) {
		this.instanceId = instanceId;

		const enabled = config.get('telemetry.enabled') as boolean;
		if (enabled) {
			this.client = new TelemetryClient(
				config.get('telemetry.config.backend.key') as string,
				config.get('telemetry.config.backend.url') as string,
			);

			this.pulseIntervalReference = setInterval(async () => {
				void this.pulse();
			}, 1 * 60 * 1000); // every hour
		}
	}

	private async pulse(): Promise<void> {
		if (!this.client) {
			return;
		}

		Object.keys(this.executionCountsBuffer).forEach(async (workflowId) => {
			await this.track('Workflow execution count', {
				workflow_id: workflowId,
				...this.executionCountsBuffer[workflowId],
			});
		});

		this.executionCountsBuffer = {};

		await this.track('pulse');
	}

	async trackWorkflowExecution(properties: IDataObject): Promise<void> {
		if (this.client) {
			const workflowId = properties.workflow_id as string;
			this.executionCountsBuffer[workflowId] = this.executionCountsBuffer[workflowId] ?? {
				manual_error_count: 0,
				manual_success_count: 0,
				prod_error_count: 0,
				prod_success_count: 0,
			};

			if (properties.success === false) {
				// errored exec
				await this.track('Workflow execution errored', properties);

				if (properties.is_manual) {
					this.executionCountsBuffer[workflowId].manual_error_count++;
				} else {
					this.executionCountsBuffer[workflowId].prod_error_count++;
				}
			} else if (properties.is_manual) {
				this.executionCountsBuffer[workflowId].manual_success_count++;
			} else {
				this.executionCountsBuffer[workflowId].prod_success_count++;
			}
		}
	}

	async trackN8nStop(): Promise<void> {
		clearInterval(this.pulseIntervalReference);
		await this.pulse();
		await this.track('User instance stopped');
	}

	async identify(traits?: IDataObject): Promise<void> {
		if (this.client) {
			await this.client.identify({
				userId: this.instanceId,
				anonymousId: '000000000000',
				traits: {
					...traits,
					instanceId: this.instanceId,
				},
			});
		}
	}

	async track(eventName: string, properties?: IDataObject): Promise<void> {
		if (this.client) {
			await this.client.track({
				userId: this.instanceId,
				event: eventName,
				anonymousId: '000000000000',
				properties,
			});
		}
	}
}
