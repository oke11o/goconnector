# goconnector

Wrappers for native connections

Using

```


	var config publisher.Config
	readYaml(`config.yaml`, &config)

	in := make(chan []byte, 10000)
	errs := make(chan error)

	var wg sync.WaitGroup

	wg.Add(1)
	go publisher.Publish(&wg, in, errs, 1, config)
    
```