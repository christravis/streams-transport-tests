package com.github.christravis.streams.transport_tests.java;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamingData.Punctuation;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.InputPortSet.WindowMode;
import com.ibm.streams.operator.model.InputPortSet.WindowPunctuationInputMode;
import com.ibm.streams.operator.model.InputPorts;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPortSet.WindowPunctuationOutputMode;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.PrimitiveOperator;

@PrimitiveOperator(name="SendDataAsTuples", namespace="com.github.christravis.streams.transport_tests.java", description="Java Operator SendDataAsTuples")
@InputPorts({@InputPortSet(description="Port that ingests tuples", cardinality=1, optional=false, windowingMode=WindowMode.NonWindowed, windowPunctuationInputMode=WindowPunctuationInputMode.Oblivious), @InputPortSet(description="Optional input ports", optional=true, windowingMode=WindowMode.NonWindowed, windowPunctuationInputMode=WindowPunctuationInputMode.Oblivious)})
@OutputPorts({@OutputPortSet(description="Port that produces tuples", cardinality=1, optional=false, windowPunctuationOutputMode=WindowPunctuationOutputMode.Generating), @OutputPortSet(description="Optional output ports", optional=true, windowPunctuationOutputMode=WindowPunctuationOutputMode.Generating)})
public class SendDataAsTuples extends AbstractOperator
{

	private List<Tuple> cache = new ArrayList<Tuple>();

	@Override
	public synchronized void initialize(OperatorContext context) throws Exception
	{
		super.initialize(context);
		Logger.getLogger(this.getClass()).trace("Operator " + context.getName() + " initializing in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId());
	}

	@Override
	public synchronized void allPortsReady() throws Exception
	{
		OperatorContext context = getOperatorContext();
		Logger.getLogger(this.getClass()).trace("Operator " + context.getName() + " all ports are ready in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId());
	}

	@Override
	public final void process(StreamingInput<Tuple> inputStream, Tuple tuple) throws Exception
	{
		this.cache.add(tuple.asReadOnlyTuple());
	}

	@Override
	public void processPunctuation(StreamingInput<Tuple> stream, Punctuation mark) throws Exception
	{
		StreamingOutput<OutputTuple> outputStream = this.getOutput(0);
		while (true) {
			for (Tuple record : this.cache) {
				OutputTuple out = outputStream.newTuple();
				out.assign(record);
				outputStream.submit(out);
			}
		}
	}

	public synchronized void shutdown() throws Exception
	{
		OperatorContext context = getOperatorContext();
		Logger.getLogger(this.getClass()).trace("Operator " + context.getName() + " shutting down in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId());
		super.shutdown();
	}

}