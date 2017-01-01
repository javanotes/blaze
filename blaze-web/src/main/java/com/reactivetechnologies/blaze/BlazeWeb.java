package com.reactivetechnologies.blaze;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

@SpringBootApplication(scanBasePackageClasses = {Config.class})
public class BlazeWeb {

	public static void main(String[] args) {
		new SpringApplicationBuilder()
	    .sources(BlazeWeb.class)
	    .registerShutdownHook(true)
	    //.bannerMode(org.springframework.boot.Banner.Mode.OFF)
	    .build(args)
	    .run(args);
	}
}
