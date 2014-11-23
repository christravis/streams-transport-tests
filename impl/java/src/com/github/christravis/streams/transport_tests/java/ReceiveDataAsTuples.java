package com.github.christravis.streams.transport_tests.java;

import org.apache.log4j.Logger;

import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.StreamingData.Punctuation;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.InputPortSet.WindowMode;
import com.ibm.streams.operator.model.InputPortSet.WindowPunctuationInputMode;
import com.ibm.streams.operator.model.InputPorts;
import com.ibm.streams.operator.model.PrimitiveOperator;

@PrimitiveOperator(name="ReceiveDataAsTuples", namespace="com.github.christravis.streams.transport_tests.java", description="Java Operator ReceiveDataAsTuples")
@InputPorts({@InputPortSet(description="Port that ingests tuples", cardinality=1, optional=false, windowingMode=WindowMode.NonWindowed, windowPunctuationInputMode=WindowPunctuationInputMode.Oblivious), @InputPortSet(description="Optional input ports", optional=true, windowingMode=WindowMode.NonWindowed, windowPunctuationInputMode=WindowPunctuationInputMode.Oblivious)})
public class ReceiveDataAsTuples extends AbstractOperator
{

	private long counter = 0l;
	private long threshold = 1000000l;

	@Override
	public synchronized void initialize(OperatorContext context) throws Exception
	{
		super.initialize(context);
		Logger.getLogger(this.getClass()).trace("Operator " + context.getName() + " initializing in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );
	}

	@Override
	public synchronized void allPortsReady() throws Exception
	{
		OperatorContext context = getOperatorContext();
		Logger.getLogger(this.getClass()).trace("Operator " + context.getName() + " all ports are ready in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );
	}

	@Override
	public void process(StreamingInput<Tuple> stream, Tuple tuple) throws Exception
	{
		this.counter++;
		if (this.counter >= this.threshold) {
			Logger.getLogger(this.getClass()).error("Got " + this.threshold + " records at: " + (System.currentTimeMillis() / 1000));
			this.counter = 0l;
		}
	}

	@Override
	public void processPunctuation(StreamingInput<Tuple> stream, Punctuation mark) throws Exception
	{
	}

	@Override
	public synchronized void shutdown() throws Exception
	{
		OperatorContext context = getOperatorContext();
		Logger.getLogger(this.getClass()).trace("Operator " + context.getName() + " shutting down in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );
		super.shutdown();
	}

}