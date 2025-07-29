cat > bridge_create_sqs_quques_part_1.sh << 'EOF'
#!/bin/bash

REGION="ca-central-1"

echo "==============================================="
echo "Evidence Transfer SQS Queue Creation Script"
echo "==============================================="
echo "Region: $REGION"
echo "User: $(aws sts get-caller-identity --query 'Arn' --output text)"
echo

# Define queue configurations (name -> visibility_timeout)
declare -A QUEUE_CONFIGS
QUEUE_CONFIGS[q-case-found]="300"
QUEUE_CONFIGS[q-case-detail]="300"
QUEUE_CONFIGS[q-evidence-download]="900"
QUEUE_CONFIGS[q-evidence-download-oversize]="3600"
QUEUE_CONFIGS[q-case-metadata-update]="300"
QUEUE_CONFIGS[q-transfer-prepare]="300"

echo "Creating queues with configurations:"
for queue_name in "${!QUEUE_CONFIGS[@]}"; do
    echo "  ${queue_name}.fifo -> ${QUEUE_CONFIGS[$queue_name]} seconds visibility timeout"
done
echo

# Step 1: Create all Dead Letter Queues first
echo "STEP 1: Creating Dead Letter Queues..."
echo "======================================"
for queue_name in "${!QUEUE_CONFIGS[@]}"; do
    echo "Creating ${queue_name}-dlq.fifo..."
    
    aws sqs create-queue \
        --queue-name "${queue_name}-dlq.fifo" \
        --attributes '{
            "FifoQueue":"true",
            "ContentBasedDeduplication":"true",
            "MessageRetentionPeriod":"1209600",
            "KmsMasterKeyId":"aws/sqs"
        }' \
        --region $REGION > /dev/null 2>&1
    
    if [ $? -eq 0 ]; then
        echo "  ✅ ${queue_name}-dlq.fifo created"
    else
        echo "  ⚠️  ${queue_name}-dlq.fifo may already exist"
    fi
done

echo
echo "Waiting 5 seconds for DLQs to be ready..."
sleep 5

# Step 2: Create main queues with minimal attributes first
echo "STEP 2: Creating Main Queues (Basic)..."
echo "======================================="
for queue_name in "${!QUEUE_CONFIGS[@]}"; do
    echo "Creating ${queue_name}.fifo..."
    
    aws sqs create-queue \
        --queue-name "${queue_name}.fifo" \
        --attributes '{
            "FifoQueue":"true",
            "ContentBasedDeduplication":"true",
            "KmsMasterKeyId":"aws/sqs"
        }' \
        --region $REGION > /dev/null 2>&1
    
    if [ $? -eq 0 ]; then
        echo "  ✅ ${queue_name}.fifo created"
    else
        echo "  ⚠️  ${queue_name}.fifo may already exist"
    fi
done

echo
echo "Waiting 5 seconds for main queues to be ready..."
sleep 5

# Step 3: Configure queue attributes (timeouts, DLQ linkage, etc.)
echo "STEP 3: Configuring Queue Attributes..."
echo "======================================="
for queue_name in "${!QUEUE_CONFIGS[@]}"; do
    visibility_timeout="${QUEUE_CONFIGS[$queue_name]}"
    echo "Configuring ${queue_name}.fifo..."
    
    # Get queue URL and DLQ ARN
    MAIN_URL=$(aws sqs get-queue-url --queue-name "${queue_name}.fifo" --region $REGION --output text 2>/dev/null)
    DLQ_URL=$(aws sqs get-queue-url --queue-name "${queue_name}-dlq.fifo" --region $REGION --output text 2>/dev/null)
    
    if [ -z "$MAIN_URL" ] || [ -z "$DLQ_URL" ]; then
        echo "  ❌ Could not get queue URLs for ${queue_name}"
        continue
    fi
    
    DLQ_ARN=$(aws sqs get-queue-attributes --queue-url "$DLQ_URL" --attribute-names QueueArn --region $REGION --query 'Attributes.QueueArn' --output text 2>/dev/null)
    
    # Configure main queue attributes
    aws sqs set-queue-attributes \
        --queue-url "$MAIN_URL" \
        --attributes "{
            \"VisibilityTimeoutSeconds\":\"$visibility_timeout\",
            \"ReceiveMessageWaitTimeSeconds\":\"20\",
            \"MessageRetentionPeriod\":\"1209600\",
            \"RedrivePolicy\":\"{\\\"deadLetterTargetArn\\\":\\\"$DLQ_ARN\\\",\\\"maxReceiveCount\\\":3}\"
        }" \
        --region $REGION > /dev/null 2>&1
    
    if [ $? -eq 0 ]; then
        echo "  ✅ ${queue_name}.fifo configured (${visibility_timeout}s timeout, DLQ linked)"
    else
        echo "  ⚠️  ${queue_name}.fifo basic config may have failed, trying minimal..."
        
        # Fallback: set just visibility timeout
        aws sqs set-queue-attributes \
            --queue-url "$MAIN_URL" \
            --attributes "{\"VisibilityTimeoutSeconds\":\"$visibility_timeout\"}" \
            --region $REGION > /dev/null 2>&1
        
        if [ $? -eq 0 ]; then
            echo "      ✅ Visibility timeout set, DLQ can be linked manually"
        else
            echo "      ❌ Configuration failed - will use defaults"
        fi
    fi
done

echo
echo "STEP 4: Verification..."
echo "======================"

# Count total queues
TOTAL_QUEUES=$(aws sqs list-queues --region $REGION --query 'QueueUrls[?contains(@, `q-`)]' | grep -c "https")
echo "Total queues created: $TOTAL_QUEUES (expected: 12)"
echo

# Verify each queue pair
echo "Queue Verification:"
for queue_name in "${!QUEUE_CONFIGS[@]}"; do
    echo "📨 ${queue_name}:"
    
    MAIN_URL=$(aws sqs get-queue-url --queue-name "${queue_name}.fifo" --region $REGION --output text 2>/dev/null)
    DLQ_URL=$(aws sqs get-queue-url --queue-name "${queue_name}-dlq.fifo" --region $REGION --output text 2>/dev/null)
    
    if [ ! -z "$MAIN_URL" ]; then
        echo "   ✅ Main: $(basename $MAIN_URL)"
        
        # Check visibility timeout
        VT=$(aws sqs get-queue-attributes --queue-url "$MAIN_URL" --attribute-names VisibilityTimeout --query 'Attributes.VisibilityTimeout' --output text 2>/dev/null)
        echo "      Visibility Timeout: ${VT}s"
    else
        echo "   ❌ Main: MISSING"
    fi
    
    if [ ! -z "$DLQ_URL" ]; then
        echo "   ✅ DLQ:  $(basename $DLQ_URL)"
    else
        echo "   ❌ DLQ:  MISSING"
    fi
    echo
done

echo "🎉 Queue creation script completed!"
echo
echo "Next steps:"
echo "1. Verify queues in AWS Console: SQS service"
echo "2. Check that DLQ linkages are working"
echo "3. Configure Lambda function triggers"
echo
echo "All queue URLs:"
aws sqs list-queues --region $REGION --query 'QueueUrls[?contains(@, `q-`)]' --output table

EOF

# Make executable and run
chmod +x create-all-queues.sh
./create-all-queues.sh