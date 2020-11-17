faust -A hello_world worker -l info

faust --app hello_world agents
faust --app hello_world livecheck
faust --app hello_world models
faust --app hello_world tables
faust --app hello_world completion

faust --app hello_world send @print_greetings "Hello Faust"
faust --app hello_world send print_greetings "Hello Kafka topic"
