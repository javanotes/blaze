/**
 * Copyright 2017 esutdal

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */
package com.reactivetechnologies.blaze;

import org.springframework.beans.BeanUtils;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.web.servlet.ViewResolver;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;
import org.thymeleaf.spring4.SpringTemplateEngine;
import org.thymeleaf.spring4.view.ThymeleafView;
import org.thymeleaf.spring4.view.ThymeleafViewResolver;
import org.thymeleaf.templateresolver.ClassLoaderTemplateResolver;
import org.thymeleaf.templateresolver.TemplateResolver;
@Configuration
@EnableWebMvc
public class WebConfig extends WebMvcConfigurerAdapter {

	@Override
    public void addResourceHandlers(ResourceHandlerRegistry registry) {
        registry.addResourceHandler("/webjars/**").addResourceLocations("classpath:/META-INF/resources/webjars/");
        registry.addResourceHandler("/images/**").addResourceLocations("classpath:/static/images/");
    }
	
	@Bean
	  public TemplateResolver getTemplateResolver() {
	    ClassLoaderTemplateResolver resolver = BeanUtils
	        .instantiate(ClassLoaderTemplateResolver.class);
	    resolver.setPrefix("templates/");
	    resolver.setSuffix(".html");
	    resolver.setTemplateMode("HTML5");
	    resolver.setOrder(1);
	    return resolver;
	  }

	  @Bean
	  public ViewResolver getTilesViewResolver() {
	    ThymeleafViewResolver viewResolver = BeanUtils
	        .instantiate(ThymeleafViewResolver.class);
	    
	    viewResolver.setViewClass(ThymeleafView.class);
	    
	    viewResolver.setTemplateEngine(getTemplateEngine());
	    viewResolver.setExcludedViewNames(new String[] { "webjars/*" });
	    viewResolver.setOrder(Ordered.HIGHEST_PRECEDENCE);
	    
	    return viewResolver;
	  }
	  
	  @Bean
	  public SpringTemplateEngine getTemplateEngine() {
	    SpringTemplateEngine templateEngine = BeanUtils
	        .instantiate(SpringTemplateEngine.class);
	    templateEngine.setTemplateResolver(getTemplateResolver());
	    
	    return templateEngine;
	  }

}
