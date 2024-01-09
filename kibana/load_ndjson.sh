until curl -s http://kibana:5601/login -o /dev/null; do
    echo Waiting for Kibana...
    sleep 10
done

# The import service may not be ready, sleep 10s more.
sleep 10
curl -X POST kibana:5601/api/saved_objects/_import -H "kbn-xsrf: true" --form file=@/tmp/load/export.ndjson

echo 'Done ! Kibana is ready.'