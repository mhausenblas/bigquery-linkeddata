$(function(){
	
	// setup
	$("#qlcontrol").text("Hide queries ...");
	$("#impcontrol").text("Import ...");

	// events
	$("#saveq").click(function () {
		saveQuery();
	});
	
	/*
	$("#execq").click(function () {
		executeQuery();
	});
	*/
	
	$(".execqsaved").live("click", function() {
		var queryID = $(this).parent().attr("id");
		var querStr = $("div[id='"+ queryID +"'] div.sq").text();
		$("#querystr").val(querStr); // set the query string in the text area
		//executeQuery();
		//alert("Executing " + queryID);
	});
	
	$(".deleteqsaved").live("click", function() {
		var queryID = $(this).parent().attr("id");
		deleteQuery(queryID);
		//alert("Deleting " + $(this).parent().attr("id"));
	});
	
	$(".hinfo").live("mouseenter", function() {
		$(this).css("background", "#f0f0ff")
	});

	$(".hinfo").live("mouseout", function() {
		$(this).css("background", "#ffffff")
	});	
	
	$("#queryfilter").keyup(function () {
		var searchterm = $("#filterq").val();
		
		if(searchterm.length > 1) {
			$("div.savedquery").each(function() {
				$(this).hide();
			});
			$("div.savedquery:contains('" + searchterm + "')").show();
		}
		else {
			$("div.savedquery").each(function() {
				$(this).show();
			});
		}
	 });
	
	$("#resetfilter").click(function () {
		$("#filterq").val("");
		$("div.savedquery").each(function() {
			$(this).show();
		});
	 });

	$("#schemehintcontrol").click(function () {
		if($("#schemehint").is(":visible")){
			$("#schemehint").slideUp("slow");
			$("#schemehintcontrol").text("view ...");
		}
		else {
			$("#schemehint").slideDown("slow");
			$("#schemehintcontrol").text("hide ...");
		}
	});
	
	$("#qlcontrol").click(function () {
		if($("#querylist").is(":visible")){
			$("#querylist").slideUp("slow");
			$("#querylisthead").slideUp("slow");
			$("#qlcontrol").text("Queries ...");
		}
		else {
			$("#querylist").slideDown("slow");
			$("#querylisthead").slideDown("slow");
			$("#qlcontrol").text("Hide queries ...");
		}
	});
	
	$("#impcontrol").click(function () {
		if($("#importf").is(":visible")){
			$("#importf").slideUp("slow");
			$("#impcontrol").text("Import ...");
		}
		else {
			$("#importf").slideDown("slow");
			$("#impcontrol").text("Hide import ...");
		}
	});
});


function saveQuery(){
	var qstrval = $("#querystr").val();
	// validate
	if(qstrval == "") {
		alert("Ah! You try to save an empty query. I don't like this ...");
		return false;
	}
	$.ajax({
		url : "/saveq",
		type: "POST",
		data: "querystr=" + escape(qstrval),
		success : function(data) {
			$("#result").html("Result:" + data);
		},
		error: function(xhr, textStatus, errorThrown){
			alert("Error: " + textStatus);
		}
	});
}

function executeQuery(){
	var qstrval = $("#querystr").val();
	// validate
	if(qstrval == "") {
		alert("Ah! You try to execute an empty query. I don't like this ...");
		return false;
	}
	$.ajax({
		url : "/execq",
		type: "POST",
		data: "querystr=" + escape(qstrval),
		success : function(data) {
		},
		error: function(xhr, textStatus, errorThrown){
			alert("Error: " + textStatus);
		}
	});
}

function deleteQuery(qID){
	$.ajax({
		url : "/deleteq",
		type: "POST",
		data: "querid=" + qID,
		success : function(data) {
			$("#result").html("Result: " + data);
			location.reload();
		},
		error: function(xhr, textStatus, errorThrown){
			alert("Error: " + textStatus);
		}
	});
}